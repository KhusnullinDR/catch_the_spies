import os
from collections import defaultdict

import pandas as pd

IN_PATH = "dim_person_fixed.parquet"
OUT_PATH = "dim_person_merged.parquet"

df = pd.read_parquet(IN_PATH).reset_index(drop=True)
n = len(df)
print(f"Загружено: {n} строк из {IN_PATH}")

NAME_INTERPRETATION = {
    "ALBINA": "ALBINA", "AL'BINA": "ALBINA",
    "ALEKSANDR": "ALEKSANDR", "ALEXANDR": "ALEKSANDR",
    "ALEKSANDRA": "ALEKSANDRA", "ALEXANDRA": "ALEKSANDRA",
    "ALEKSEY": "ALEKSEY", "ALEKSEI": "ALEKSEY", "ALEXEY": "ALEKSEY",
    "ALENA": "ALENA", "ALINA": "ALINA", "ALISA": "ALISA",
    "ALIYA": "ALIYA", "ALIIA": "ALIYA", "ALLA": "ALLA",
    "AMALIYA": "AMALIYA", "AMALIIA": "AMALIYA",
    "AMELIYA": "AMELIYA", "AMELIIA": "AMELIYA",
    "AMINA": "AMINA", "AMIR": "AMIR",
    "ANASTASIYA": "ANASTASIYA", "ANASTASIIA": "ANASTASIYA",
    "ANATOLIY": "ANATOLIY", "ANATOLII": "ANATOLIY",
    "ANDREY": "ANDREY", "ANDREI": "ANDREY",
    "ANGELINA": "ANGELINA", "ANNA": "ANNA",
    "ANTON": "ANTON", "ANTONINA": "ANTONINA",
    "ANZHELIKA": "ANZHELIKA", "ARIANA": "ARIANA", "ARINA": "ARINA",
    "ARSEN": "ARSEN", "ARSENIY": "ARSENIY", "ARSENII": "ARSENIY",
    "ARTEM": "ARTEM", "ARTEMIY": "ARTEMIY", "ARTEMII": "ARTEMIY",
    "ARTUR": "ARTUR", "AYDAR": "AYDAR", "AIDAR": "AYDAR",
    "AGATA": "AGATA", "ADEL": "ADEL", "ADEL'": "ADEL",
    "ADELINA": "ADELINA", "ALBERT": "ALBERT", "AL'BERT": "ALBERT",
    "BOGDAN": "BOGDAN", "BORIS": "BORIS",
    "DAMIR": "DAMIR", "DANIEL": "DANIEL", "DANIEL'": "DANIEL",
    "DANIIL": "DANIIL", "DANIL": "DANIL", "DANILA": "DANILA",
    "DARIA": "DARYA", "DAR'YA": "DARYA", "DARINA": "DARINA",
    "DAVID": "DAVID", "DEMID": "DEMID", "DENIS": "DENIS",
    "DIANA": "DIANA", "DMITRIY": "DMITRIY", "DMITRII": "DMITRIY",
    "EDUARD": "EDUARD", "EGOR": "EGOR", "EKATERINA": "EKATERINA",
    "ELENA": "ELENA", "ELINA": "ELINA",
    "ELISEI": "ELISEY", "ELISEY": "ELISEY", "ELIZAVETA": "ELIZAVETA",
    "ELMIRA": "ELMIRA", "EL'MIRA": "ELMIRA",
    "ELVIRA": "ELVIRA", "EL'VIRA": "ELVIRA",
    "EMIL": "EMIL", "EMIL'": "EMIL",
    "EMILIYA": "EMILIYA", "EMILIIA": "EMILIYA",
    "ESENIYA": "ESENIYA", "ESENIIA": "ESENIYA",
    "EVA": "EVA", "EVELINA": "EVELINA",
    "EVGENIY": "EVGENIY", "EVGENII": "EVGENIY",
    "EVGENIYA": "EVGENIYA", "EVGENIIA": "EVGENIYA",
    "FEDOR": "FEDOR", "FILIPP": "FILIPP",
    "GALINA": "GALINA",
    "GEORGIY": "GEORGIY", "GEORGII": "GEORGIY",
    "GERMAN": "GERMAN", "GLEB": "GLEB",
    "GORDEY": "GORDEY", "GORDEI": "GORDEY",
    "GRIGORIY": "GRIGORIY", "GRIGORII": "GRIGORIY",
    "IAN": "YAN", "IANA": "YANA",
    "IAROMIR": "YAROMIR", "IAROSLAV": "YAROSLAV",
    "IAROSLAVA": "YAROSLAVA", "IASMINA": "YASMINA",
    "IGOR": "IGOR", "IGOR'": "IGOR", "IGNAT": "IGNAT",
    "ILDAR": "ILDAR", "IL'DAR": "ILDAR",
    "ILIA": "ILYA", "IL'YA": "ILYA",
    "INNA": "INNA", "IRINA": "IRINA",
    "IULIANA": "YULIANA", "IULIIA": "YULIYA",
    "IURII": "YURIY", "IVAN": "IVAN",
    "JANA": "YANA", "JULIA": "YULIYA", "JULIYA": "YULIYA",
    "JULYANA": "YULIANA", "JULIANA": "YULIANA",
    "JURIJ": "YURIY", "JURIY": "YURIY",
    "JEVGENIJ": "EVGENIY", "JEVGENIY": "EVGENIY",
    "JELENA": "ELENA", "JELIZAVETA": "ELIZAVETA",
    "JAROSLAV": "YAROSLAV", "JAROSLAVA": "YAROSLAVA",
    "JAROMIR": "YAROMIR",
    "KAMILLA": "KAMILLA", "KARINA": "KARINA",
    "KIRA": "KIRA", "KIRILL": "KIRILL",
    "KONSTANTIN": "KONSTANTIN", "KRISTINA": "KRISTINA",
    "KSENIYA": "KSENIYA", "KSENIIA": "KSENIYA",
    "KSENIA": "KSENIYA",
    "XENIYA": "KSENIYA", "XENIA": "KSENIYA",
    "LARISA": "LARISA", "LEONID": "LEONID", "LEV": "LEV",
    "LIANA": "LIANA",
    "LIDIYA": "LIDIYA", "LIDIIA": "LIDIYA",
    "LILIYA": "LILIYA", "LILIIA": "LILIYA",
    "LIUBOV": "LYUBOV", "LYUBOV": "LYUBOV", "LYUBOV'": "LYUBOV",
    "LIUDMILA": "LYUDMILA", "LYUDMILA": "LYUDMILA",
    "MADINA": "MADINA", "MAIIA": "MAYA", "MAYYA": "MAYA",
    "MAKAR": "MAKAR", "MAKSIM": "MAKSIM", "MAXIM": "MAKSIM",
    "MARGARITA": "MARGARITA",
    "MARIANA": "MARYANA", "MARYANA": "MARYANA", "MAR'YANA": "MARYANA",
    "MARIYA": "MARIYA", "MARIIA": "MARIYA",
    "MARK": "MARK", "MARSEL": "MARSEL", "MARSEL'": "MARSEL",
    "MARINA": "MARINA", "MARAT": "MARAT",
    "MATVEY": "MATVEY", "MATVEI": "MATVEY",
    "MELANIYA": "MELANIYA", "MELANIIA": "MELANIYA",
    "MIKHAIL": "MIKHAIL", "MILANA": "MILANA",
    "MILENA": "MILENA", "MIRON": "MIRON",
    "MIROSLAV": "MIROSLAV", "MIROSLAVA": "MIROSLAVA",
    "NADEZHDA": "NADEZHDA",
    "NATALIA": "NATALYA", "NATAL'YA": "NATALYA",
    "NATALIYA": "NATALYA", "NATALIIA": "NATALYA",
    "NATALYA": "NATALYA", "NATASHA": "NATASHA",
    "NAZAR": "NAZAR", "NELLI": "NELLI", "NIKA": "NIKA",
    "NIKITA": "NIKITA",
    "NIKOLAI": "NIKOLAY", "NIKOLAY": "NIKOLAY",
    "NINA": "NINA",
    "ODISSEI": "ODISSEY", "ODISSEY": "ODISSEY",
    "OKSANA": "OKSANA", "OXANA": "OKSANA",
    "OLEG": "OLEG",
    "OLESIA": "OLESYA", "OLESYA": "OLESYA",
    "OL'GA": "OLGA", "OLGA": "OLGA",
    "PAVEL": "PAVEL", "PETR": "PETR",
    "PLATON": "PLATON", "POLINA": "POLINA",
    "RADMIR": "RADMIR",
    "RAMIL": "RAMIL", "RAMIL'": "RAMIL",
    "REGINA": "REGINA", "RINAT": "RINAT",
    "ROBERT": "ROBERT", "RODION": "RODION",
    "ROMAN": "ROMAN", "ROSTISLAV": "ROSTISLAV",
    "RUSLAN": "RUSLAN", "RUSTAM": "RUSTAM",
    "SABINA": "SABINA", "SAMIR": "SAMIR", "SAMIRA": "SAMIRA",
    "SAVELIY": "SAVELIY", "SAVELII": "SAVELIY",
    "SAVVA": "SAVVA", "SEMEN": "SEMEN",
    "SERGEI": "SERGEY", "SERGEY": "SERGEY",
    "SNEZHANA": "SNEZHANA",
    "SOFIA": "SOFIYA",
    "SOFYA": "SOFIYA",
    "SOFIYA": "SOFIYA",
    "SOF'YA": "SOFIYA",
    "SOFIIA": "SOFIYA",
    "STANISLAV": "STANISLAV",
    "STEFANIYA": "STEFANIYA", "STEFANIIA": "STEFANIYA",
    "STEPAN": "STEPAN", "SVETLANA": "SVETLANA",
    "SVIATOGOR": "SVYATOGOR", "SVYATOGOR": "SVYATOGOR",
    "SVIATOSLAV": "SVYATOSLAV", "SVYATOSLAV": "SVYATOSLAV",
    "TAISIYA": "TAISIYA", "TAISIIA": "TAISIYA",
    "TAMARA": "TAMARA", "TAMERLAN": "TAMERLAN",
    "TATIANA": "TATYANA", "TAT'YANA": "TATYANA", "TATYANA": "TATYANA",
    "TIKHON": "TIKHON", "TIMOFEI": "TIMOFEY", "TIMOFEY": "TIMOFEY",
    "TIMUR": "TIMUR",
    "ULIANA": "ULYANA", "UL'YANA": "ULYANA", "ULYANA": "ULYANA",
    "VADIM": "VADIM",
    "VALERIY": "VALERIY", "VALERII": "VALERIY",
    "VALERIYA": "VALERIYA", "VALERIIA": "VALERIYA",
    "VALENTINA": "VALENTINA", "VARVARA": "VARVARA",
    "VASILII": "VASILIY", "VASILIY": "VASILIY",
    "VASILISA": "VASILISA", "VASILINA": "VASILINA",
    "VERA": "VERA", "VERONIKA": "VERONIKA",
    "VIACHESLAV": "VYACHESLAV", "VYACHESLAV": "VYACHESLAV",
    "VIKTOR": "VIKTOR",
    "VIKTORIYA": "VIKTORIYA", "VIKTORIIA": "VIKTORIYA",
    "VIOLETTA": "VIOLETTA",
    "VITALIY": "VITALIY", "VITALII": "VITALIY",
    "VITALINA": "VITALINA",
    "VLAD": "VLAD", "VLADA": "VLADA",
    "VLADIMIR": "VLADIMIR",
    "VLADISLAV": "VLADISLAV", "VLADISLAVA": "VLADISLAVA",
    "VSEVOLOD": "VSEVOLOD",
    "YANA": "YANA", "YAROMIR": "YAROMIR",
    "YAROSLAV": "YAROSLAV", "YAROSLAVA": "YAROSLAVA",
    "YASMINA": "YASMINA",
    "YULIANA": "YULIANA", "YULIYA": "YULIYA",
    "YURIY": "YURIY",
    "ZAKHAR": "ZAKHAR", "ZARINA": "ZARINA", "ZLATA": "ZLATA",
}
CANONICAL_NAMES = set(NAME_INTERPRETATION.values())
print(f"Словарь имён: {len(NAME_INTERPRETATION)} вариантов -> {len(CANONICAL_NAMES)} канонов")

APOSTROPHES = ("'", "’", "‘", "`", "´", "\u200b", "\u200c", "\u200d", "\ufeff")


def is_null(x):
    if x is None:
        return True
    try:
        if pd.isna(x):
            return True
    except (TypeError, ValueError):
        pass
    return str(x).strip().lower() in ("null", "none", "", "<na>")


def strip_apostrophes(s):
    for a in APOSTROPHES:
        s = s.replace(a, "")
    return s


def norm_key(x):
    if is_null(x):
        return ""
    return strip_apostrophes(str(x).strip().upper())


def canonical_name(x):
    k = norm_key(x)
    if not k:
        return ""
    return NAME_INTERPRETATION.get(k, k)


def canonical_surname(x):
    k = norm_key(x)
    if not k:
        return ""
    k = k.replace("X", "KS")
    k = k.replace("TC", "TS")
    k = k.replace("IU", "YU")
    if k.startswith("LI"):
        k = "LY" + k[2:]
    k = k.replace("AI", "AY")
    k = k.replace("OI", "OY")
    k = k.replace("IA", "YA")
    if k.endswith("II"):
        k = k[:-2] + "IY"
    k = k.replace("EI", "EY")
    k = k.replace("II", "IY")
    if k.endswith("YY") or k.endswith("YI"):
        k = k[:-2] + "IY"
    return k


def clean_value(x):
    if is_null(x):
        return x
    return strip_apostrophes(str(x))


def first_letter_mid(x):
    if is_null(x):
        return ""
    s = norm_key(x)
    return s[0] if s else ""


parent = list(range(n))


def find(a):
    while parent[a] != a:
        parent[a] = parent[parent[a]]
        a = parent[a]
    return a


def union(a, b):
    ra, rb = find(a), find(b)
    if ra != rb:
        parent[rb] = ra


last_c = [canonical_surname(x) for x in df["last_name"].tolist()]
first_c = [canonical_name(x) for x in df["first_name"].tolist()]
gen = [norm_key(x) for x in df["gender"].tolist()]

mid1 = [first_letter_mid(x) for x in df["middle_name"].tolist()]
ff_air = [norm_key(x) for x in df["ff_airline"].tolist()]
ff_num = [norm_key(x) for x in df["ff_number"].tolist()]

print("Склейка по (last_name_canon, first_name_canon, gender) + правило middle_name ...")

buckets = defaultdict(list)
for i in range(n):
    if last_c[i] and first_c[i] and gen[i]:
        buckets[(last_c[i], first_c[i], gen[i])].append(i)


def ff_compatible(i, j):
    ai, ni = ff_air[i], ff_num[i]
    aj, nj = ff_air[j], ff_num[j]
    if ai and ni and aj and nj:
        return (ai, ni) == (aj, nj)
    return True


for key, idxs in buckets.items():
    if len(idxs) < 2:
        continue

    by_mid1 = defaultdict(list)
    empty_mid = []
    for i in idxs:
        if mid1[i]:
            by_mid1[mid1[i]].append(i)
        else:
            empty_mid.append(i)

    for m1, lst in by_mid1.items():
        anchor = lst[0]
        for k in lst[1:]:
            if ff_compatible(anchor, k):
                union(anchor, k)

    if by_mid1:
        first_m1 = next(iter(by_mid1))
        anchor = by_mid1[first_m1][0]
        for i in empty_mid:
            if ff_compatible(anchor, i):
                union(anchor, i)
    else:
        if len(empty_mid) > 1:
            anchor = empty_mid[0]
            for i in empty_mid[1:]:
                if ff_compatible(anchor, i):
                    union(anchor, i)

print("Сборка эталонов ...")

clusters = defaultdict(list)
for i in range(n):
    clusters[find(i)].append(i)

print(f"Кластеров (уникальных личностей): {len(clusters)}")

col_names = ["person_id", "last_name", "first_name", "middle_name",
             "gender", "ff_number", "ff_airline", "birth_date",
             "passport", "passport_country", "ff_program_name",
             "forum_uid", "forum_registered_flights_count",
             "contact_phone", "contact_email", "source_file"]
cols = {c: df[c].tolist() for c in col_names if c in df.columns}
notnull = {c: [0 if is_null(x) else 1 for x in cols[c]] for c in cols}

records = []
for root, idxs in clusters.items():
    def sort_key(i):
        has_mid = notnull["middle_name"][i]
        score = (notnull["last_name"][i] + notnull["first_name"][i]
                 + has_mid + notnull["gender"][i]
                 + notnull["ff_number"][i] + notnull["ff_airline"][i])
        return (-has_mid, -score, cols["person_id"][i])

    order = sorted(idxs, key=sort_key)
    ref_i = order[0]
    ref = {c: cols[c][ref_i] for c in cols}

    if is_null(ref.get("middle_name")):
        for k in order:
            if not is_null(cols["middle_name"][k]):
                ref["middle_name"] = cols["middle_name"][k]
                break

    for c in ("ff_airline", "ff_number"):
        if c in ref and is_null(ref[c]):
            for k in order:
                if not is_null(cols[c][k]):
                    ref[c] = cols[c][k]
                    break

    for c in ("last_name", "first_name", "middle_name",
              "ff_airline", "ff_number", "ff_program_name",
              "passport", "passport_country", "contact_phone",
              "contact_email", "source_file"):
        if c in ref:
            ref[c] = clean_value(ref[c])

    all_ids = sorted(int(cols["person_id"][i]) for i in idxs)
    ref["id_duplicate"] = all_ids
    ref["is_duplicate_group"] = len(all_ids) > 1
    records.append(ref)

df_merged = pd.DataFrame(records)

base_cols = [c for c in df.columns if c in df_merged.columns]
front = ["person_id", "id_duplicate", "is_duplicate_group"]
other = [c for c in base_cols if c not in front]
df_merged = df_merged[front + other]

print(f"Итог: {len(df_merged)} строк (было {n})")
print(f"Групп с дубликатами: {int(df_merged['is_duplicate_group'].sum())}")
print(f"Строк, вошедших в дубликаты: "
      f"{int(df_merged['id_duplicate'].map(len).where(df_merged['is_duplicate_group'], 0).sum())}")

assert sum(len(x) for x in df_merged["id_duplicate"]) == n, \
    "Потерялись или задвоились person_id при склейке!"
print("Проверка целостности: OK")

df_merged.to_parquet(OUT_PATH, index=False)
print(f"Сохранено: {OUT_PATH}")

dups_csv = "dim_person_merged_duplicates.csv"
df_merged[df_merged["is_duplicate_group"]].to_csv(dups_csv, index=False, encoding="utf-8")
print(f"Отчёт по дубликатам: {dups_csv}")

print("\nКОНФЛИКТЫ ПО ПАРАМ (ff_airline, ff_number)")

df_by_pid = {int(r["person_id"]): r for r in df.to_dict("records")}

ff_groups = defaultdict(list)
for i in range(n):
    if ff_air[i] and ff_num[i]:
        ff_groups[(ff_air[i], ff_num[i])].append(int(cols["person_id"][i]))

print(f"Уникальных пар (ff_airline, ff_number): {len(ff_groups)}")


def canon_surname_field(a, b, field):
    va = a.get(field); vb = b.get(field)
    na = is_null(va); nb = is_null(vb)
    if na and nb: return False
    if na != nb: return True
    return canonical_surname(va) != canonical_surname(vb)


def canon_name_field(a, b, field):
    va = a.get(field); vb = b.get(field)
    na = is_null(va); nb = is_null(vb)
    if na and nb: return False
    if na != nb: return True
    return canonical_name(va) != canonical_name(vb)


def plain_field(a, b, field):
    va = a.get(field); vb = b.get(field)
    na = is_null(va); nb = is_null(vb)
    if na and nb: return False
    if na != nb: return True
    return norm_key(va) != norm_key(vb)


pair_conflicts = []
conflict_pairs_count = 0
conflict_records_count = 0
clean_pairs_count = 0

for (air, num), pids in ff_groups.items():
    if len(pids) < 2:
        continue
    rows_list = [df_by_pid[pid] for pid in pids]
    diffs = set()
    m = len(rows_list)
    for i in range(m):
        for j in range(i + 1, m):
            if canon_surname_field(rows_list[i], rows_list[j], "last_name"):
                diffs.add("last_name")
            if canon_name_field(rows_list[i], rows_list[j], "first_name"):
                diffs.add("first_name")
            if plain_field(rows_list[i], rows_list[j], "gender"):
                diffs.add("gender")
            m1 = first_letter_mid(rows_list[i].get("middle_name"))
            m2 = first_letter_mid(rows_list[j].get("middle_name"))
            if m1 and m2 and m1 != m2:
                diffs.add("middle_name(1st letter)")
    if diffs:
        conflict_pairs_count += 1
        conflict_records_count += len(pids)
        pair_conflicts.append({
            "airline": air,
            "number": num,
            "ids": sorted(pids),
            "diff_fields": sorted(diffs),
            "rows": rows_list,
        })
    else:
        clean_pairs_count += 1

print(f"Пар без расхождений: {clean_pairs_count}")
print(f"Пар с расхождениями: {conflict_pairs_count}")
print(f"Записей в конфликтных парах: {conflict_records_count}")

if pair_conflicts:
    txt_path = "dim_person_conflicts_by_ff.txt"
    pair_conflicts.sort(key=lambda g: (-len(g["diff_fields"]), g["airline"], g["number"]))

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("КОНФЛИКТЫ ПО ПАРАМ (ff_airline, ff_number)\n")
        f.write("Фамилии: canonical_surname (X->KS, TC->TS, IU->YU, LI->LY, "
                "AI->AY, OI->OY, IA->YA, II->IY, EI->EY, YY/YI->IY)\n")
        f.write("Имена: NAME_INTERPRETATION\n")
        f.write("Отчество: конфликт только если у обеих записей 1-я буква непустая и РАЗНАЯ "
                "(nan vs буква — не конфликт, это штатное дополнение отчества)\n")
        f.write("Апострофы при сравнении и записи игнорируются.\n")
        f.write("=" * 70 + "\n")
        f.write(f"Уникальных пар (airline, number):       {len(ff_groups)}\n")
        f.write(f"Пар без расхождений:                    {clean_pairs_count}\n")
        f.write(f"Пар с расхождениями:                    {conflict_pairs_count}\n")
        f.write(f"Записей в конфликтных парах:            {conflict_records_count}\n")
        f.write("=" * 70 + "\n\n")

        for idx, g in enumerate(pair_conflicts, 1):
            f.write(f"### ПАРА #{idx}: airline={g['airline']} | number={g['number']}\n")
            f.write(f"    Записей в паре:      {len(g['ids'])}\n")
            f.write(f"    person_id:           {g['ids']}\n")
            f.write(f"    Расходящиеся поля:   {', '.join(g['diff_fields'])}\n")
            f.write("    Строки:\n")
            for r in g["rows"]:
                ln = clean_value(r.get("last_name"))
                fn = clean_value(r.get("first_name"))
                mn = clean_value(r.get("middle_name"))
                f.write(
                    f"      person_id={r.get('person_id')} | "
                    f"last={ln} | first={fn} | "
                    f"mid={mn} | gender={r.get('gender')}\n"
                )
            f.write("\n")
    print(f"\nКонфликты по парам выгружены в: {txt_path}")
else:
    print("\nКонфликтных пар — не найдено.")