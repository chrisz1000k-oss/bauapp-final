import io
import hashlib
from datetime import datetime

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet


def generate_weekly_pdf(*, company_name: str, week_id: str, employee_display: str, rows_df, signature_text: str) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=A4,
        leftMargin=1.5 * cm, rightMargin=1.5 * cm,
        topMargin=1.2 * cm, bottomMargin=1.2 * cm,
    )
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph(f"<b>{company_name}</b>", styles["Title"]))
    elements.append(Paragraph(f"Wochenrapport: <b>{week_id}</b>", styles["Heading2"]))
    elements.append(Paragraph(f"Mitarbeiter: <b>{employee_display}</b>", styles["Normal"]))
    elements.append(Paragraph(f"Erstellt: {datetime.now().strftime('%d.%m.%Y %H:%M')}", styles["Normal"]))
    elements.append(Spacer(1, 0.4 * cm))

    table_data = [["Datum", "Projekt", "Arbeitsbeschrieb", "Material", "Std."]]
    for _, r in rows_df.iterrows():
        table_data.append([
            str(r.get("datum", "") or ""),
            str(r.get("projekt_name", "") or ""),
            str(r.get("arbeitsbeschrieb", "") or ""),
            str(r.get("material", "") or ""),
            str(r.get("hours", "") or ""),
        ])

    t = Table(table_data, colWidths=[2.2 * cm, 4.0 * cm, 7.5 * cm, 3.2 * cm, 1.2 * cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.8 * cm))

    elements.append(Paragraph("<b>Visum Mitarbeiter</b>", styles["Heading3"]))
    elements.append(Paragraph(signature_text, styles["Normal"]))

    raw = (week_id + employee_display + rows_df.to_csv(index=False)).encode("utf-8")
    doc_hash = hashlib.sha256(raw).hexdigest()[:16]
    elements.append(Spacer(1, 0.8 * cm))
    elements.append(Paragraph(
        f"<font size=8 color=grey>Doc-ID: {doc_hash} · UTC {datetime.utcnow().isoformat(timespec='seconds')}</font>",
        styles["Normal"]
    ))

    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()

