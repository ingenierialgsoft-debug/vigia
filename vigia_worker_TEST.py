import re
import time
from playwright.sync_api import sync_playwright, TimeoutError

CPNU_URL = "https://consultaprocesos.ramajudicial.gov.co/Procesos/NumeroRadicacion"
RADICADO_TEST = "17001333300220250044400"
LABEL_TODOS = "Todos los Procesos (consulta completa, menos rápida)"

SLOW_MO_MS = 180
PAUSE_S = 0.30

def pause(sec=PAUSE_S):
    time.sleep(sec)

def log(msg: str):
    print(msg, flush=True)

def safe_wait(locator, label: str, timeout=15000) -> bool:
    try:
        locator.wait_for(timeout=timeout)
        return True
    except Exception as e:
        log(f"   [wait-fail] {label}: {e}")
        return False

def safe_click(locator, label: str, timeout=15000) -> bool:
    try:
        locator.wait_for(timeout=timeout)
        locator.click(timeout=timeout)
        return True
    except Exception as e1:
        log(f"   [fallback] click normal falló en '{label}': {e1}")
        try:
            locator.wait_for(timeout=timeout)
            locator.click(timeout=timeout, force=True)
            return True
        except Exception as e2:
            log(f"   [error] click forzado falló en '{label}': {e2}")
            return False

def safe_fill(locator, value: str, label: str, timeout=15000) -> bool:
    try:
        locator.wait_for(timeout=timeout)
        locator.fill(value, timeout=timeout)
        return True
    except Exception as e1:
        log(f"   [fallback] fill falló en '{label}': {e1}")
        try:
            locator.click(timeout=timeout, force=True)
            locator.fill(value, timeout=timeout)
            return True
        except Exception as e2:
            log(f"   [error] click+fill falló en '{label}': {e2}")
            return False

def click_consultar(page):
    """
    CPNU (Vuetify) a veces no expone bien role/button. Hacemos:
    A) CSS Vuetify: button.v-btn:has-text("CONSULTAR")
    B) role=button name=CONSULTAR
    C) texto exacto CONSULTAR
    """
    log("5) Consultando (click CONSULTAR)")

    # A) selector Vuetify
    btn_css = page.locator("button.v-btn:has-text('CONSULTAR')").first
    if safe_click(btn_css, "CONSULTAR_CSS_VUETIFY"):
        pause()
        return

    # B) role button
    btn_role = page.get_by_role("button", name=re.compile("^CONSULTAR$", re.I))
    if safe_click(btn_role, "CONSULTAR_ROLE"):
        pause()
        return

    # C) texto
    btn_text = page.get_by_text(re.compile("^CONSULTAR$", re.I))
    if safe_click(btn_text, "CONSULTAR_TEXT"):
        pause()
        return

    raise RuntimeError("No fue posible hacer click en CONSULTAR con ningún método.")

def modal_no_results_aparece(page) -> bool:
    try:
        page.get_by_text(re.compile("La consulta no generó resultados", re.I)).wait_for(timeout=3500)
        return True
    except TimeoutError:
        return False

def cerrar_modal_volver(page):
    log("6) Modal sin resultados detectado -> click VOLVER")

    # A) CSS vuetify botón
    volver_css = page.locator("button.v-btn:has-text('VOLVER')").first
    if not safe_click(volver_css, "VOLVER_CSS"):
        # B) role button
        volver_role = page.get_by_role("button", name=re.compile("^VOLVER$", re.I))
        if not safe_click(volver_role, "VOLVER_ROLE"):
            # C) texto
            volver_text = page.get_by_text(re.compile("^VOLVER$", re.I))
            if not safe_click(volver_text, "VOLVER_TEXT"):
                raise RuntimeError("No fue posible hacer click en VOLVER.")

    # Esperar que el modal desaparezca (si no, seguimos igual)
    try:
        page.get_by_text(re.compile("La consulta no generó resultados", re.I)).wait_for(state="detached", timeout=8000)
    except TimeoutError:
        log("   [fallback] modal no se desprendió explícitamente; continuo")
    pause(0.4)

def seleccionar_todos_procesos(page):
    log("7) Seleccionando radio 'Todos los Procesos' (Vuetify)")

    label = page.get_by_text(LABEL_TODOS, exact=True)
    if not safe_click(label, "LABEL_TODOS"):
        raise RuntimeError("No fue posible hacer click en el label de 'Todos los Procesos'.")

    # Validar por input asociado (atributo for)
    input_id = None
    try:
        input_id = label.evaluate("el => el.getAttribute('for')")
    except Exception:
        input_id = None

    if input_id:
        radio_input = page.locator(f"#{input_id}")
        aria_checked = radio_input.get_attribute("aria-checked")
        checked_prop = radio_input.evaluate("el => el.checked")

        if not (checked_prop or aria_checked == "true"):
            label.click(force=True)
            pause(0.2)
            aria_checked = radio_input.get_attribute("aria-checked")
            checked_prop = radio_input.evaluate("el => el.checked")

        assert (checked_prop or aria_checked == "true"), "No se pudo seleccionar 'Todos los Procesos'."
    else:
        # Fallback por role radio
        radio = page.get_by_role("radio", name=re.compile("Todos los Procesos", re.I))
        try:
            radio.wait_for(timeout=8000)
            try:
                radio.check(timeout=2000)
            except Exception:
                radio.click(force=True)
            assert radio.is_checked()
        except Exception as e:
            raise RuntimeError(f"No se pudo validar selección de 'Todos': {e}")

    log("   OK: radio 'Todos los Procesos' quedó seleccionado")
    pause(0.2)

def esperar_tabla_resultados(page):
    log("8) Esperando tabla de resultados")
    # Ancla real de tu HTML: thead con columnheader "Número de Radicación"
    # A) role columnheader
    th_role = page.get_by_role("columnheader", name=re.compile("Número de Radicación", re.I))
    if safe_wait(th_role, "TH_NUMERO_RADICACION_ROLE", timeout=60000):
        pause(0.2)
        return

    # B) thead texto
    th_text = page.locator("thead").get_by_text(re.compile("Número de Radicación", re.I))
    if safe_wait(th_text, "TH_NUMERO_RADICACION_THEAD", timeout=60000):
        pause(0.2)
        return

    raise RuntimeError("No se detectó la tabla de resultados.")

def click_radicado_en_tabla(page):
    log("9) Click en el radicado (botón Vuetify dentro de la tabla)")
    # Según tu HTML: td 2 tiene un button azul con span.v-btn__content
    btn = page.locator("table tbody tr td:nth-child(2) button").first
    if safe_wait(btn, "RADICADO_BTN_ESTRUCTURAL", timeout=60000):
        try:
            txt = btn.locator("span.v-btn__content").inner_text().strip()
            log(f"   Radicado encontrado: '{txt}'")
        except Exception:
            pass

        if safe_click(btn, "RADICADO_BTN_CLICK", timeout=15000):
            pause(0.6)
            return

    # Fallback por contenido del span
    span = page.locator("table tbody tr td:nth-child(2) span.v-btn__content", has_text=RADICADO_TEST).first
    if safe_click(span, "RADICADO_SPAN_CLICK", timeout=15000):
        pause(0.6)
        return

    raise RuntimeError("No se pudo hacer click en el radicado en la tabla de resultados.")

def esperar_tabs_detalle(page):
    log("10) Esperando vista detalle (tabs)")
    tab_act = page.get_by_role("tab", name=re.compile("^Actuaciones$", re.I))
    if safe_wait(tab_act, "TAB_ACTUACIONES_ROLE", timeout=60000):
        return

    tab_div = page.locator("div[role='tab']", has_text=re.compile("^Actuaciones$", re.I)).first
    if safe_wait(tab_div, "TAB_ACTUACIONES_DIV", timeout=60000):
        return

    raise RuntimeError("No se detectó la vista de detalle (tabs).")

def click_tab_actuaciones(page):
    log("11) Click en tab Actuaciones")
    tab_act = page.get_by_role("tab", name=re.compile("^Actuaciones$", re.I))
    if safe_click(tab_act, "TAB_ACTUACIONES_ROLE", timeout=15000):
        pause(0.6)
        return

    tab_div = page.locator("div[role='tab']", has_text=re.compile("^Actuaciones$", re.I)).first
    if safe_click(tab_div, "TAB_ACTUACIONES_DIV", timeout=15000):
        pause(0.6)
        return

    # Último fallback: texto
    if safe_click(page.get_by_text(re.compile("^Actuaciones$", re.I)), "TAB_ACTUACIONES_TEXT", timeout=15000):
        pause(0.6)
        return

    raise RuntimeError("No se pudo hacer click en la pestaña Actuaciones.")

def esperar_tabla_actuaciones(page):
    log("12) Esperando tabla de actuaciones")
    # A) texto header
    t = page.get_by_text(re.compile("Fecha de Actuación", re.I))
    if safe_wait(t, "ACT_TEXTO_FECHA_ACTUACION", timeout=60000):
        return

    # B) thead
    th = page.locator("thead").get_by_text(re.compile("Fecha de Actuación", re.I))
    if safe_wait(th, "ACT_THEAD_FECHA_ACTUACION", timeout=60000):
        return

    raise RuntimeError("No se detectó la tabla de actuaciones.")

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=SLOW_MO_MS)
        context = browser.new_context(viewport={"width": 1400, "height": 900})
        page = context.new_page()

        try:
            log("1) Abriendo CPNU")
            page.goto(CPNU_URL, wait_until="domcontentloaded", timeout=60000)
            pause()

            log("2) Esperando pantalla Número de Radicación")
            # anclas: título o placeholder
            if not safe_wait(page.get_by_text(re.compile("Número de Radicación", re.I)), "TITULO_NUMERO_RADICACION", timeout=60000):
                if not safe_wait(page.get_by_placeholder(re.compile("23 dígitos", re.I)), "PLACEHOLDER_23_DIGITOS", timeout=60000):
                    raise RuntimeError("No se detectó la pantalla de Número de Radicación.")
            pause(0.2)

            log("3) Seleccionando 'Actuaciones Recientes (30 días)'")
            radio_rec = page.get_by_role("radio", name=re.compile("Actuaciones Recientes", re.I))
            try:
                radio_rec.check(timeout=6000)
            except Exception:
                # fallback: click texto
                safe_click(page.get_by_text(re.compile("Actuaciones Recientes", re.I)), "RADIO_RECIENTES_TEXT", timeout=15000)
            pause(0.2)

            log("4) Ingresando radicado")
            inp = page.get_by_placeholder(re.compile("23 dígitos", re.I))
            if not safe_fill(inp, RADICADO_TEST, "INPUT_RADICADO"):
                # fallback: primer textbox
                safe_fill(page.get_by_role("textbox").first, RADICADO_TEST, "INPUT_RADICADO_FALLBACK")
            pause(0.2)

            click_consultar(page)

            if modal_no_results_aparece(page):
                cerrar_modal_volver(page)
                seleccionar_todos_procesos(page)
                click_consultar(page)
            else:
                log("6) No apareció modal, sigue flujo normal")

            esperar_tabla_resultados(page)
            click_radicado_en_tabla(page)

            esperar_tabs_detalle(page)
            click_tab_actuaciones(page)
            esperar_tabla_actuaciones(page)

            log("OK) Recorrido completo. Dejando el navegador abierto 8 segundos.")
            time.sleep(8)

        except Exception as e:
            log(f"ERROR: {e}")
            try:
                page.screenshot(path="cpnu_visual_error.png", full_page=True)
                log("Se guardó screenshot: cpnu_visual_error.png")
            except Exception:
                pass
            time.sleep(8)

        finally:
            context.close()
            browser.close()

if __name__ == "__main__":
    main()
