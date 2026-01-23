1. Stammdaten Produkte & Lager
Produkte/Artikel aus Lager.csv und Mengenstu-eckliste-Table.normalized.csv zu einer sauberen products.csv zusammenführen und importieren (Modell product.template / product.product).
​

Wichtig: default_code = deine IDs wie 000.1.000, 018.2.000, 019.2.008, 020.2.000, 029.3.000 usw.

2. Lieferanten & product.supplierinfo
Lieferanten.csv als Partner/Lieferanten importieren (Modell res.partner, Typ „Lieferant“).
​
po
Danach product.supplierinfo.csv mit Referenz auf product_tmpl_id/default_code und name/id importieren, damit jeder Kaufartikel seinen Lieferanten und Preis hat.
​

3. Stücklisten (BoMs) für die drei EVO-Varianten
bom.csv mit den drei Köpfen 029.3.000/001/002 und allen Komponentenzeilen importieren (Modell mrp.bom).
​

Prüfen: In der BoM‑Maske sollte pro Variante die vollständige Stückliste (Elektronik, Hauben, Grundplatten, Füße, Verpackung usw.) sichtbar sein.

4. Workcenter und Operationen
Workcenter (3D‑Drucker, Lasercutter, Nacharbeit, Elektronik‑Montage, Gehäuse & Rotoren, Qualität) anlegen oder per CSV importieren.
​
​

CSV mit Operationen (op_3ddruck_haube, op_nacharbeit_fuesse, op_qualitaetskontrolle usw.) für mrp.routing.workcenter importieren, damit die Arbeitsgänge existieren.
​

5. Operationen an die BoMs hängen
In den BoMs der EVO‑Endprodukte 029.3.000/001/002 die Operationen in der gewünschten Reihenfolge eintragen (3D‑Druck → Lasern → Nacharbeit → Elektronik‑Montage → Gehäuse/Rotoren → Endkontrolle).
​
​

Damit erzeugt Odoo bei jedem Fertigungsauftrag Work Orders an den richtigen Workcentern.

6. Quality Control Points importieren
quality.point.csv mit den generierten IDs (qp_haube_*, qp_fuss_*, qp_gp_*, qp_endtest_*) importieren, verknüpft mit product_id/default_code und operation_id/id.
​

Ergebnis: Beim Start der entsprechenden Work Orders werden automatisch Mess‑/Prüfmasken für Haube, Füße, Grundplatten und Endtest angezeigt.
