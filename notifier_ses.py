# notifier_ses.py
import os
import traceback
import boto3
from botocore.exceptions import BotoCoreError, ClientError

SES_REGION = os.getenv("AWS_REGION", "us-east-1")
SES_SENDER = os.getenv("SES_SENDER")  # e.g. no-reply@tudominio.com (verificado en SES)
SES_RECIPIENTS = [e.strip() for e in os.getenv("SES_RECIPIENTS", "").split(",") if e.strip()]

_ses = boto3.client("ses", region_name=SES_REGION)

def _send_email(subject: str, text: str, html: str | None = None) -> bool:
    if not SES_SENDER or not SES_RECIPIENTS:
        print("[SES] Falta SES_SENDER o SES_RECIPIENTS; se omite envío.")
        return False
    body = {"Text": {"Data": text, "Charset": "UTF-8"}}
    if html:
        body["Html"] = {"Data": html, "Charset": "UTF-8"}
    try:
        _ses.send_email(
            Source=SES_SENDER,
            Destination={"ToAddresses": SES_RECIPIENTS},
            Message={"Subject": {"Data": subject, "Charset": "UTF-8"}, "Body": body},
        )
        print(f"[SES] Enviado: {subject}")
        return True
    except (BotoCoreError, ClientError) as e:
        print(f"[SES] ERROR enviando email: {e}")
        return False

def notify_success(state_code: str, state_name: str, added: int, api_requests: int,
                   sheet_tab: str, sheet_id: str):
    subject = f"[StudioFinder] {state_code} OK – {added} nuevos"
    text = (
        f"Estado: {state_name} ({state_code})\n"
        f"Pestaña en Sheet: {sheet_tab}\n"
        f"Nuevos lugares añadidos: {added}\n"
        f"Solicitudes API: {api_requests}\n"
        f"Sheet: https://docs.google.com/spreadsheets/d/{sheet_id}\n"
    )
    _send_email(subject, text)

def notify_failure(state_code: str, state_name: str, err: Exception):
    subject = f"[StudioFinder] {state_code} ERROR – ejecución interrumpida"
    tb = "".join(traceback.format_exception(type(err), err, err.__traceback__))
    text = (
        f"Estado: {state_name} ({state_code})\n"
        f"Tipo: {type(err).__name__}\n"
        f"Error: {err}\n\n"
        f"Traceback:\n{tb}\n"
    )
    _send_email(subject, text)

def notify_summary(done_items: list[tuple[str, str, int, int]]):
    # done_items: [(state_code, state_name, added, api_requests)]
    total_added = sum(i[2] for i in done_items)
    total_reqs = sum(i[3] for i in done_items)
    subject = f"[StudioFinder] Resumen OK – {len(done_items)} estados, {total_added} nuevos"
    lines = [
        f"Estados procesados: {len(done_items)}",
        f"Nuevos lugares totales: {total_added}",
        f"Total API requests: {total_reqs}",
        "",
        "Detalle por estado:",
    ]
    for sc, sn, added, reqs in done_items:
        lines.append(f" - {sc} ({sn}): +{added} | reqs={reqs}")
    _send_email(subject, "\n".join(lines))
