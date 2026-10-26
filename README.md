# Batch-Datenarchitektur für Taxinachfrage

Portfolio im Kurs Projekt: Data Engineering an der IU Internationale Hochschule.

- Name: Rahil Chughtai
- Kurs: Projekt: Data Engineering
- Kursnummer: DLMDWWDE02
- Aufgabe: 1.1, batch-basierte Datenarchitektur für eine datenintensive Applikation
- Datenquelle: [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Konzept

Für das Projekt wurde Aufgabe 1.1 gewählt: Ziel ist eine Datenpipeline, die einmal pro Quartal aufbereitete Trainingsdaten zur Taxinachfrage für eine ML-Anwendung bereitstellt. Als Datenquelle dient der [NYC TLC Yellow Taxi Trip Datensatz](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), der detaillierte Aufzeichnungen von Taxifahrten veröffentlicht.

Das System basiert auf einer containerisierten Microservice-Architektur, die durch Nutzung von Docker Compose und Git reproduzierbar ist. Die Architektur gliedert sich in folgende Kernkomponenten:

* Ingestion & Storage: Ein Python-basierter Ingestion-Service lädt die historischen Monatsdaten herunter und speichert sie in einem S3-kompatiblen SeaweedFS-Objektspeicher, der als strukturierter Data Lake (Bronze-, Silver- und Gold-Layer) dient.
* Processing: Ein verteilter Apache-Spark-Cluster bereinigt und validiert die Millionen von Datensätzen und lässt sich durch zusätzliche Worker horizontal skalieren. Die aggregierten Features werden als optimierte Parquet-Dateien im Gold-Layer abgelegt und somit der ML-Anwendung bereitgestellt.
* Orchestrierung: Der Ablauf wird über ein modulares Python-Skript gesteuert, das die Services nacheinander startet.

Die Verlässlichkeit ergibt sich aus idempotenten Schritten, automatischen Retries, Healthchecks und Datenprüfungen. Die Wartbarkeit wird durch einen Service pro Container, gepinnte Versionen und Tests gewährleistet. Datenschutz und Governance werden durch Datenminimierung, getrennte Zugriffsrechte, Qualitätsprüfungen und nachvollziehbare, versionierte Datenstände umgesetzt.

## Architektur

![Architekturdiagramm der Batch-Pipeline](figures/architektur.png)

Die editierbare Quelle des Diagramms liegt in [figures/architektur.drawio](figures/architektur.drawio).
