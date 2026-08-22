"""Generate mock MSCI/GICS taxonomy drops for testing the `msci-test` feed.

One archive per business day, several member files inside each -- the shape the
real feed has, and the one `target.partition_by: [business_date]` relies on. Each
member states its own date in a `# Generated:` comment and its own row count in a
`TRLR_COUNT` trailer, so every gate in `ffe dry-run` has something to check.

    uv run python msci-test-gen.py                    # 7 days into data/msci_test
    uv run python msci-test-gen.py --days 14 --parts 4
    uv run python msci-test-gen.py --drift --bad-rows # exercise the failure paths

Output is deterministic: the same arguments always produce the same bytes, so a
regenerated drop does not show up as spurious churn.
"""

from __future__ import annotations

import argparse
import shutil
import zipfile
from datetime import date, timedelta
from pathlib import Path

# A slice of the GICS tree: sector / industry group / industry / sub-industry,
# labelled in both languages, because the accented French is what makes this
# feed a UTF-8 test as well as a structure test.
TAXONOMY: list[tuple[str, str, str, str, str, str]] = [
    ("10", "1010", "101010", "10101010", "Energy (Oil & Gas)", "Énergie"),
    ("10", "1010", "101020", "10102010", "Oil, Gas & Consumable Fuels", "Combustibles"),
    ("15", "1510", "151010", "15101010", "Commodity Chemicals", "Produits Chimiques"),
    ("15", "1510", "151040", "15104020", "Gold", "Or"),
    ("15", "1510", "151050", "15105010", "Containers & Packaging", "Emballages"),
    ("20", "2010", "201010", "20101010", "Aerospace & Defense", "Aérospatiale"),
    ("20", "2010", "201060", "20106010", "Industrial Machinery", "Machines Industrielles"),
    ("20", "2030", "203020", "20302010", "Passenger Airlines", "Compagnies Aériennes"),
    ("25", "2510", "251010", "25101010", "Auto Components", "Composants Automobiles"),
    ("25", "2510", "251020", "25102010", "Automobiles", "Automobiles"),
    ("25", "2550", "255030", "25503020", "Apparel Retail", "Vêtements"),
    ("30", "3010", "301010", "30101010", "Food & Staples Retailing", "Distribution Alimentaire"),
    ("30", "3020", "302020", "30202010", "Packaged Foods & Meats", "Produits Alimentaires"),
    ("30", "3030", "303010", "30301010", "Household Products", "Produits Ménagers"),
    ("35", "3510", "351010", "35101010", "Health Care Equipment & Supplies", "Équipement Médical"),
    ("35", "3520", "352010", "35201010", "Biotechnology", "Biotechnologie"),
    ("35", "3520", "352020", "35202010", "Pharmaceuticals", "Produits Pharmaceutiques"),
    ("40", "4010", "401010", "40101010", "Banks", "Banques"),
    ("40", "4020", "402010", "40201040", "Consumer Finance", "Crédit à la Consommation"),
    ("40", "4030", "403010", "40301040", "Property & Casualty Insurance", "Assurance Dommages"),
    ("45", "4510", "451020", "45102010", "IT Consulting & Other Services", "Conseil Informatique"),
    ("45", "4510", "451030", "45103020", "Systems Software", "Logiciels Systèmes"),
    ("45", "4520", "452010", "45201020", "Communications Equipment", "Équipement de Comms"),
    ("45", "4530", "453010", "45301020", "Semiconductors", "Semi-conducteurs"),
    ("50", "5010", "501010", "50101020", "Movies & Entertainment", "Cinéma et Divertissement"),
    ("50", "5020", "502010", "50201040", "Interactive Media & Services", "Médias Interactifs"),
    ("55", "5510", "551010", "55101010", "Electric Utilities", "Services Électriques"),
    ("55", "5510", "551030", "55103010", "Multi-Utilities", "Services Multiples"),
    ("60", "6010", "601010", "60101020", "Industrial REITs", "SIIC Industrielles"),
    ("60", "6010", "601060", "60106010", "Retail REITs", "SIIC Commerciales"),
]

HEADER = "Sector_ID|Industry_Group_ID|Industry_ID|Sub_Industry_ID::Label_EN::Label_FR"
# The drift column only appears from --drift-on onwards, so a run can prove that
# policy.schema_change fails by default and evolves when asked.
HEADER_DRIFT = HEADER + "::Region"


def row(entry: tuple[str, str, str, str, str, str], region: str | None) -> str:
    sector, group, industry, sub, label_en, label_fr = entry
    tail = f"{label_en}::{label_fr}" + (f"::{region}" if region else "")
    return f"{sector}|{group}|{industry}|{sub}::{tail}"


def member(day: date, entries: list, drift: bool, bad: int) -> str:
    """One file: comment block, schema, data, trailer.

    TRLR_COUNT counts every data line including the malformed ones -- the
    trailer describes what the sender put in the file, not what a parser
    manages to read. That is what makes `trailer_row_count` worth trusting.
    """
    region = "EMEA" if drift else None
    lines = [row(e, region) for e in entries]
    # A shape the parser must reject rather than crash on: the `::` tail is
    # missing its third part, so the row cannot be split into columns.
    lines += [f"99|9999|999999|9999{n:04d}::Malformed Row {n}" for n in range(bad)]

    return "\n".join(
        [
            "# Source Reference: S&P Dow Jones Indices & MSCI Taxonomy Setup",
            f"# Generated: {day.isoformat()}",
            "# Encoding: UTF-8",
            "",
            "[SCHEMA_START]",
            HEADER_DRIFT if drift else HEADER,
            "[DATA_BLOCK]",
            *lines,
            "[SCHEMA_END]",
            f"TRLR_COUNT:{len(lines)}",
            "",
        ]
    )


def slice_for(day_index: int, parts: int) -> list[list]:
    """Deterministic, different every day, never empty.

    Rotating the pool rather than sampling it keeps the output reproducible
    while still giving each day its own rows -- so a partitioned table has
    something distinguishable in each partition.
    """
    offset = (day_index * 7) % len(TAXONOMY)
    rotated = TAXONOMY[offset:] + TAXONOMY[:offset]
    take = rotated[: max(parts, len(TAXONOMY) - day_index % 5)]
    return [take[i::parts] for i in range(parts)]


def build(args: argparse.Namespace) -> list[Path]:
    out = Path(args.out)
    if args.clean and out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    start = date.fromisoformat(args.start)
    written = []
    for index in range(args.days):
        day = start + timedelta(days=index)
        if args.weekdays_only and day.weekday() >= 5:
            continue

        drift = args.drift and index >= args.drift_on
        stamp = day.strftime("%Y%m%d")
        path = out / f"taxonomy_{stamp}.zip"

        # ZIP_DEFLATED and a fixed date_time so regenerating gives identical
        # bytes; zipfile otherwise stamps "now" into every entry.
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            for part, entries in enumerate(slice_for(index, args.parts), start=1):
                bad = args.bad_rows if part == 1 else 0
                info = zipfile.ZipInfo(
                    f"taxonomy_setup_{stamp}_{part:02d}.txt",
                    date_time=(day.year, day.month, day.day, 0, 0, 0),
                )
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, member(day, entries, drift, bad))
        written.append(path)
    return written


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--out", default="data/msci_test", help="output directory")
    p.add_argument("--start", default="2026-08-17", help="first business date")
    p.add_argument("--days", type=int, default=7, help="how many days to generate")
    p.add_argument("--parts", type=int, default=3, help="member files per archive")
    p.add_argument(
        "--weekdays-only", action="store_true", help="skip Saturday and Sunday"
    )
    p.add_argument(
        "--drift",
        action="store_true",
        help="add a Region column from --drift-on onwards, to exercise "
        "policy.schema_change",
    )
    p.add_argument("--drift-on", type=int, default=4, help="day index drift starts")
    p.add_argument(
        "--bad-rows",
        type=int,
        default=0,
        help="malformed rows per archive, to exercise the reject table",
    )
    p.add_argument("--clean", action="store_true", help="empty --out first")
    args = p.parse_args()

    written = build(args)
    total = sum(len(zipfile.ZipFile(z).namelist()) for z in written)
    print(f"{len(written)} archives, {total} members -> {args.out}")
    for path in written:
        names = zipfile.ZipFile(path).namelist()
        print(f"  {path.name}  {len(names)} members")
    print(
        "\nnext:\n"
        f"  uv run ffe explain feeds/msci-test.yaml\n"
        f"  uv run ffe run feeds/msci-test.yaml --table sandbox.msci_test "
        f"--workspace ./_x"
    )


if __name__ == "__main__":
    main()
