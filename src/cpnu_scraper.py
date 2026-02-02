import re
import time
from typing import Any, Dict, List, Tuple, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from .settings import settings

CPNU_URL = "https://consultaprocesos.ramajudicial.gov.co/Procesos/NumeroRadicacion"
LABEL_TODOS = "Todos los Procesos (consulta completa, menos rápida)"

class CpnuScrapeError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message

def _modal_no_results(page) -> bool:
    try:
        page.get_by_text(re.compile("La consulta no generó resultados", re.I)).wait_for(timeout=3500)
        return True
    except PWTimeoutError:
        return False

def _click_consultar(page):
    btn = page.locator("button.v-btn:has-text('CONSULTAR')").first
    try:
        btn.wait_for(timeout=15000)
        btn.click()
        return
    except Exception:
        pass

    # fallback role/button
    try:
        page.get_by_role("button", name=re.compile("^CONSULTAR$", re.I)).click(timeout=15000)
        return
    except Exception:
        pass

    # fallback text
    page.get_by_text(re.compile("^CONSULTAR$", re.I)).click(timeout=15000)

def _select_todos(page):
    label = page.get_by_text(LABEL_TODOS, exact=True)
    label.click(timeout=15000)
    input_id = label.evaluate("el => el.getAttribute('for')")
    if input_id:
        inp = page.locator(f"#{input_id}")
        aria = inp.get_attribute("aria-checked")
        checked = inp.evaluate("el => el.checked")
        if not (checked or aria == "true"):
            label.click(force=True)

def _click_radicado_in_results(page):
    btn = page.locator("table tbody tr button",).filter(has_text=re.compile(r"\d{10,}")).first
    btn.wait_for(timeout=60000)
    btn.click(timeout=15000)

def _click_tab_actuaciones(page):
    try:
        # Intentar múltiples estrategias
        selectors = [
            "role=tab[name=/actuaciones/i]",
            "div[role='tab']:has-text('Actuaciones')",
            "button[role='tab']:has-text('Actuaciones')",
            "text=Actuaciones >> xpath=./ancestor-or-self::*[@role='tab']",
        ]
        
        for selector in selectors:
            try:
                tab = page.locator(selector).first
                tab.wait_for(timeout=5000)
                tab.click()
                time.sleep(1)  # Pequeña pausa para que se active la pestaña
                return
            except:
                continue
        
        page.locator("[role='tab']", has_text=re.compile("^Actuaciones$", re.I)).first.click(timeout=15000)
    except Exception as e:
        raise CpnuScrapeError("TAB_NOT_FOUND", f"No se pudo hacer clic en la pestaña Actuaciones: {str(e)}")

def _wait_actuaciones_table(page):
    try:
        time.sleep(2)
        table = page.locator("table").filter(
            has=page.locator("th:has-text('Fecha de Actuación')")
        ).first        
        table.wait_for(state="visible", timeout=30000)        
        # Verificar que haya filas con datos reales (no solo estructura vacía)
        page.wait_for_function(
            """() => {
                const rows = document.querySelectorAll('table tbody tr');
                if (rows.length === 0) return false;
                // Verificar que al menos una fila tenga contenido en la primera celda
                const firstRow = rows[0];
                const firstCell = firstRow.querySelector('td');
                return firstCell && firstCell.textContent.trim().length > 0;
            }""",
            timeout=30000
        )
        
        return True
        
    except PWTimeoutError as e:
        # Verificar diferentes escenarios de error
        no_results_selectors = [
            "text=/no.*resultados/i",
            "text=/sin.*datos/i",
            "text=/vacío/i",
            "text=/no.*encontrado/i",
        ]
        
        for selector in no_results_selectors:
            if page.locator(selector).count() > 0:
                raise CpnuScrapeError(
                    "NO_DATA",
                    "La tabla de actuaciones no tiene datos."
                ) from e
        
        # Verificar si hay tabla pero sin filas
        if page.locator("table").count() > 0:
            raise CpnuScrapeError(
                "EMPTY_TABLE",
                "Se encontró la tabla de actuaciones pero está vacía."
            ) from e
        
        raise CpnuScrapeError(
            "TABLE_NOT_FOUND",
            "No se pudo encontrar la tabla de actuaciones después de esperar."
        ) from e

def _extract_actuaciones_rows(page, max_rows: int) -> List[Dict[str, Any]]:
    """
    Extrae filas de la tabla en Actuaciones - VERSIÓN CORREGIDA
    Basado en el HTML proporcionado que muestra 7 columnas
    """
    rows = []
    
    try:
        # Localizar la tabla específica de actuaciones
        table = page.locator("table").filter(
            has=page.locator("th:has-text('Fecha de Actuación')")
        ).first
        
        # Obtener todas las filas del tbody
        trs = table.locator("tbody tr")
        
        count = trs.count()
        if count == 0:
            return rows
        
        take = min(count, max_rows)
        
        for i in range(take):
            tr = trs.nth(i)
            tds = tr.locator("td")
            td_count = tds.count()
            
            # Extraer datos de cada columna según la estructura del HTML
            row = {
                "fecha_actuacion": tds.nth(0).inner_text().strip() if td_count > 0 else "",
                "actuacion": tds.nth(1).inner_text().strip() if td_count > 1 else "",
                "anotacion": tds.nth(2).inner_text().strip() if td_count > 2 else "",
                "fecha_inicia_termino": tds.nth(3).inner_text().strip() if td_count > 3 else "",
                "fecha_finaliza_termino": tds.nth(4).inner_text().strip() if td_count > 4 else "",
                "fecha_registro": tds.nth(5).inner_text().strip() if td_count > 5 else "",
            }
            rows.append(row)
            
    except Exception as e:
        raise CpnuScrapeError(
            "EXTRACTION_ERROR",
            f"Error al extraer datos de la tabla: {str(e)}"
        )
    
    return rows

def scrape_actuaciones_cpnu(radicado: str) -> Tuple[List[Dict[str, Any]], str]:
    radicado = re.sub(r"\D+", "", radicado or "")
    if len(radicado) != 23:
        raise CpnuScrapeError("BAD_INPUT", "Radicado debe tener 23 dígitos.")

    used_mode = "RECIENTES"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=settings.headless)
        context = browser.new_context(viewport={"width": 1400, "height": 900})
        page = context.new_page()

        try:
            page.goto(CPNU_URL, wait_until="domcontentloaded", timeout=60000)
            
            # Esperar que la página cargue
            try:
                page.get_by_text(re.compile("Número de Radicación", re.I)).wait_for(timeout=60000)
            except Exception:
                page.get_by_placeholder(re.compile("23 dígitos", re.I)).wait_for(timeout=60000)
            
            # Seleccionar "Actuaciones Recientes" si está disponible
            try:
                page.get_by_role("radio", name=re.compile("Actuaciones Recientes", re.I)).check(timeout=6000)
            except Exception:
                pass
            
            # Ingresar el radicado
            page.get_by_placeholder(re.compile("23 dígitos", re.I)).fill(radicado)
            
            # Hacer clic en Consultar
            _click_consultar(page)
            
            # Verificar si no hay resultados
            if _modal_no_results(page):
                try:
                    page.locator("button.v-btn:has-text('VOLVER')").first.click(timeout=15000)
                except Exception:
                    page.get_by_role("button", name=re.compile("^VOLVER$", re.I)).click(timeout=15000)

                used_mode = "TODOS"
                _select_todos(page)
                _click_consultar(page)
            
            # Esperar resultados y hacer clic en el radicado
            page.get_by_role("columnheader", name=re.compile("Número de Radicación", re.I)).wait_for(timeout=60000)
            _click_radicado_in_results(page)
            
            # Navegar a la pestaña de Actuaciones
            page.get_by_role("tab", name=re.compile("^Actuaciones$", re.I)).wait_for(timeout=60000)
            _click_tab_actuaciones(page)
            
            # Esperar y extraer datos de la tabla
            _wait_actuaciones_table(page)
            rows = _extract_actuaciones_rows(page, settings.check_rows)
            
            return rows, used_mode

        except CpnuScrapeError:
            raise
        except PWTimeoutError as e:
            raise CpnuScrapeError("TIMEOUT", str(e)) from e
        except Exception as e:
            raise CpnuScrapeError("ERROR", str(e)) from e
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass