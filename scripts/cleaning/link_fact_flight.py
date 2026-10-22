import ast

import pandas as pd

DIM_PATH = "dim_person_merged.parquet"
FACT_IN = "fact_flights.parquet"
FACT_OUT = "fact_flight_remapped.parquet"

dim = pd.read_parquet(DIM_PATH)
fact = pd.read_parquet(FACT_IN)

print(f"dim_person_merged: {len(dim)} строк")
print(f"fact_flight:       {len(fact)} строк")

if "id_duplicate" not in dim.columns:
    raise KeyError("В dim_person_merged нет колонки 'id_duplicate'")

old_to_new = {}
conflicts = []

for _, row in dim.iterrows():
    new_pid = int(row["person_id"])
    ids = row["id_duplicate"]
    if ids is None:
        continue
    if isinstance(ids, str):
        try:
            ids = ast.literal_eval(ids)
        except Exception:
            ids = [ids]
    for old in ids:
        old = int(old)
        if old in old_to_new and old_to_new[old] != new_pid:
            conflicts.append((old, old_to_new[old], new_pid))
        else:
            old_to_new[old] = new_pid

print(f"Уникальных старых person_id в маппинге: {len(old_to_new)}")
print(f"Конфликтов (один старый id -> разные новые): {len(conflicts)}")
if conflicts:
    for c in conflicts[:10]:
        print("   ", c)

if "person_id" not in fact.columns:
    raise KeyError(f"В fact_flight нет 'person_id'. Колонки: {fact.columns.tolist()}")

fact_pids = fact["person_id"].dropna().astype("int64")
missing = sorted(set(fact_pids.unique()) - set(old_to_new.keys()))
print(f"\nУникальных person_id в fact_flight: {fact_pids.nunique()}")
print(f"Не найдено в маппинге:              {len(missing)}")
if missing:
    print("  Примеры отсутствующих (первые 20):", missing[:20])

fact_out = fact.copy()


def remap(pid):
    if pd.isna(pid):
        return pid
    pid = int(pid)
    return old_to_new.get(pid, pid)


fact_out["person_id"] = fact_out["person_id"].map(remap)

n_before = fact["person_id"].nunique()
n_after = fact_out["person_id"].nunique()
n_changed_rows = int((fact["person_id"].astype("Int64") !=
                      fact_out["person_id"].astype("Int64")).sum())

print(f"\nУникальных person_id ДО:    {n_before}")
print(f"Уникальных person_id ПОСЛЕ: {n_after}")
print(f"Строк, у которых person_id изменился: {n_changed_rows}")

fact_out.to_parquet(FACT_OUT, index=False)
print(f"\nСохранено: {FACT_OUT}")

report_rows = []
for old, new in old_to_new.items():
    report_rows.append({"old_person_id": old, "new_person_id": new})
report = pd.DataFrame(report_rows).sort_values("new_person_id")
report.to_csv("person_id_remap.csv", index=False, encoding="utf-8")
print(f"Отчёт по маппингу: person_id_remap.csv ({len(report)} строк)")

dim_pids = set(dim["person_id"].astype("int64"))
fact_new_pids = set(fact_out["person_id"].dropna().astype("int64"))
still_missing = fact_new_pids - dim_pids
print(f"\nПосле замены: person_id в fact, которых нет в dim: {len(still_missing)}")
if still_missing:
    print("  Примеры (первые 20):", sorted(still_missing)[:20])