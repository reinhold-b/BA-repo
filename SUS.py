#!/usr/bin/env python3
"""CLI-Tool zur Auswertung vom SUS-Fragebogen.

SUS-Logik:
- Ungerade Items (1,3,5,7,9): x - 1
- Gerade Items   (2,4,6,8,10): 5 - x
- SUS-Score = Summe * 2.5 (0 bis 100)
"""

from __future__ import annotations

import statistics
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

OUTPUT_FILE = Path(__file__).with_name("sus_results.txt")


def parse_answers(raw: str) -> List[int]:
    """Parst 10 Werte aus einem String (Leerzeichen, Komma oder Semikolon erlaubt)."""
    tokens = raw.replace(";", " ").replace(",", " ").split()
    if len(tokens) != 10:
        raise ValueError("Bitte genau 10 Werte eingeben.")

    values: List[int] = []
    for idx, token in enumerate(tokens, start=1):
        try:
            value = int(token)
        except ValueError as exc:
            raise ValueError(f"Wert {idx} ist keine ganze Zahl: {token}") from exc

        if value < 1 or value > 5:
            raise ValueError(f"Wert {idx} muss zwischen 1 und 5 liegen: {value}")
        values.append(value)

    return values


def sus_item_scores(answers: Iterable[int]) -> List[float]:
    """Berechnet die einzelnen SUS-Itemwerte nach der Standardlogik."""
    answers_list = list(answers)
    if len(answers_list) != 10:
        raise ValueError("Es werden genau 10 Antworten benötigt.")

    scores: List[float] = []
    for i, x in enumerate(answers_list, start=1):
        if i % 2 == 1:
            scores.append(float(x - 1))
        else:
            scores.append(float(5 - x))
    return scores


def sus_score(answers: Iterable[int]) -> float:
    """Berechnet den SUS-Score für 10 Antworten."""
    return sum(sus_item_scores(answers)) * 2.5


def interpret(score: float) -> str:
    """Einfache SUS-Interpretation"""
    if score < 50:
        return "Poor"
    if score < 68:
        return "OK (unterdurchschnittlich)"
    if score < 80.3:
        return "Good"
    if score < 90:
        return "Excellent"
    return "Best Imaginable"


def read_one_participant(participant_index: int) -> List[int]:
    print(f"\nTeilnehmer {participant_index}")
    print("Bitte 10 Werte (1-5) eingeben, z. B.: 4 2 5 1 4 2 5 1 4 2")

    while True:
        raw = input("Antworten: ").strip()
        try:
            return parse_answers(raw)
        except ValueError as err:
            print(f"Fehler: {err}")
            print("Bitte erneut eingeben.")


def print_single_result(index: int, score: float) -> None:
    print(f"Teilnehmer {index}: SUS = {score:.1f} / 100 ({interpret(score)})")


def print_group_stats(scores: List[float]) -> None:
    if not scores:
        return

    print("\nGruppenauswertung")
    print(f"n = {len(scores)}")
    print(f"Mittelwert: {statistics.mean(scores):.2f}")
    print(f"Median:     {statistics.median(scores):.2f}")
    print(f"Minimum:    {min(scores):.1f}")
    print(f"Maximum:    {max(scores):.1f}")
    if len(scores) > 1:
        print(f"StdAbw:     {statistics.stdev(scores):.2f}")


def write_results_to_file(participant_results: List[dict], scores: List[float]) -> None:
    """Schreibt die SUS-Ergebnisse in eine Textdatei."""
    lines: List[str] = []
    lines.append("SUS-Auswertung")
    lines.append(f"Erstellt am: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    lines.append("")

    for result in participant_results:
        lines.append(f"Teilnehmer {result['index']}:")
        lines.append(f"Antworten: {' '.join(str(v) for v in result['answers'])}")
        lines.append(
            f"Itemwerte: {' '.join(f'{v:.0f}' for v in result['item_scores'])}"
        )
        lines.append(
            f"SUS-Score: {result['score']:.1f} / 100 ({interpret(result['score'])})"
        )
        lines.append("")

    if scores:
        lines.append("Gruppenauswertung")
        lines.append(f"n = {len(scores)}")
        lines.append(f"Mittelwert: {statistics.mean(scores):.2f}")
        lines.append(f"Median:     {statistics.median(scores):.2f}")
        lines.append(f"Minimum:    {min(scores):.1f}")
        lines.append(f"Maximum:    {max(scores):.1f}")
        if len(scores) > 1:
            lines.append(f"StdAbw:     {statistics.stdev(scores):.2f}")

    OUTPUT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nErgebnisse wurden in '{OUTPUT_FILE.name}' gespeichert.")


def main() -> None:
    print("SUS-Auswertungstool")
    print("Leere Eingabe bei 'Weitere Person?' beendet die Auswertung.")

    participant = 1
    scores: List[float] = []
    participant_results: List[dict] = []

    while True:
        answers = read_one_participant(participant)
        item_scores = sus_item_scores(answers)
        score = sus_score(answers)
        scores.append(score)
        participant_results.append(
            {
                "index": participant,
                "answers": answers,
                "item_scores": item_scores,
                "score": score,
            }
        )
        print_single_result(participant, score)

        cont = input("Weitere Person auswerten? (j/n): ").strip().lower()
        if cont not in {"j", "ja", "y", "yes"}:
            break
        participant += 1

    print_group_stats(scores)
    write_results_to_file(participant_results, scores)


if __name__ == "__main__":
    main()
