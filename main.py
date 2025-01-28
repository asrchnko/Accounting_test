
from fastapi import FastAPI, UploadFile, Form, Body
from fastapi.responses import StreamingResponse
from typing import List
from starlette.background import BackgroundTask
import pandas as pd
import os
import json
from tempfile import NamedTemporaryFile

app = FastAPI(title="File Comparison API", description="Сервис для сравнения двух файлов Excel.", version="1.0")

def hash_row(row):
    return hash(tuple(row))

def load_and_fix_headers(file_path):
    df = pd.read_excel(file_path, engine='openpyxl', header=None)
    if df.iloc[0].count() == 1:
        df.columns = df.iloc[1]
        df = df[2:].reset_index(drop=True)
    else:
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
    return df

def compare_by_keys_v3(df, key_columns, source_column="Source"):
    if 'composite_key' not in df.columns:
        df['composite_key'] = df[key_columns].fillna('').apply(
            lambda row: '_'.join(row.astype(str)), axis=1
        )

    df['Differences'] = None
    grouped = df.groupby('composite_key')

    for key, group in grouped:
        rows_before = group[group[source_column] == 'Before']
        rows_after = group[group[source_column] == 'After']

        if not rows_before.empty and not rows_after.empty:
            before_rows = rows_before.to_dict('records')
            after_rows = rows_after.to_dict('records')
            
            for row_before, row_after in zip(before_rows, after_rows):
                diff = {}
                for col in df.columns:
                    if col not in key_columns + [source_column, 'composite_key', 'Differences']:
                        value_before = row_before.get(col, None)
                        value_after = row_after.get(col, None)

                        if value_before != value_after:
                            diff[col] = {"before": value_before, "after": value_after}

                idx_after = row_after['composite_key']
                idx_after = rows_after[rows_after['composite_key'] == idx_after].index[0]

                df.at[idx_after, 'Differences'] = json.dumps(diff, ensure_ascii=False) if diff else None

    return df

def compare_files_by_hash(file_before, file_after, output_file, key_columns, sort_by):
    df_before = load_and_fix_headers(file_before).applymap(str)
    df_after = load_and_fix_headers(file_after).applymap(str)

    df_before.reset_index(drop=True, inplace=True)
    df_after.reset_index(drop=True, inplace=True)

    df_before['hash'] = df_before.apply(hash_row, axis=1)
    df_after['hash'] = df_after.apply(hash_row, axis=1)

    df_before['Source'] = 'Before'
    df_after['Source'] = 'After'

    diff_before = df_before[~df_before['hash'].isin(df_after['hash'])]
    diff_after = df_after[~df_after['hash'].isin(df_before['hash'])]

    result = pd.concat([diff_before, diff_after], ignore_index=True).drop(columns='hash')

    cols = ['Source'] + [col for col in result.columns if col != 'Source']
    result = result[cols]

    if sort_by in result.columns:
        result = result.sort_values(by=sort_by)

    result = compare_by_keys_v3(result, key_columns)
    columns_order = ['Differences'] + [col for col in result.columns if col != 'Differences']

    def aggregate_differences(result):
        diffs = result['Differences'].dropna().apply(json.loads)
        field_counts = {}
        for diff in diffs:
            for key in diff.keys():
                field_counts[key] = field_counts.get(key, 0) + 1

        agg_df = pd.DataFrame(
            {"Field Name": list(field_counts.keys()), "Count": list(field_counts.values())}
        )
        return agg_df

    agg_df = aggregate_differences(result)

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        result.to_excel(writer, sheet_name='Detailed Differences', index=False)
        agg_df.to_excel(writer, sheet_name='Aggregated Differences', index=False)

@app.post(
    "/compare",
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
            "description": "Файл с результатами сравнения",
        }
    },
    summary="Сравнение двух Excel-файлов",
    description="Загрузите два Excel-файла для сравнения. В результате вы получите Excel-файл с различиями.",
)
@app.post(
    "/compare",
    responses={
        200: {
            "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
            "description": "Файл с результатами сравнения",
        }
    },
    summary="Сравнение двух Excel-файлов",
    description="Загрузите два Excel-файла для сравнения. В результате вы получите Excel-файл с различиями.",
)
async def compare_files(
    file_before: UploadFile,
    file_after: UploadFile,
    key_columns: str = Form(..., description="Колонки для ключевого сравнения, через запятую"),
    sort_by: str = Form(..., description="Колонка для сортировки"),
):
    key_columns_list = key_columns.split(',')

    # Создаем временные файлы
    temp_file_before = NamedTemporaryFile(delete=False, suffix=".xlsx")
    temp_file_after = NamedTemporaryFile(delete=False, suffix=".xlsx")
    temp_output_file = NamedTemporaryFile(delete=False, suffix=".xlsx")

    try:
        # Сохраняем входные файлы
        with open(temp_file_before.name, "wb") as f:
            f.write(await file_before.read())

        with open(temp_file_after.name, "wb") as f:
            f.write(await file_after.read())

        # Выполняем сравнение файлов
        compare_files_by_hash(temp_file_before.name, temp_file_after.name, temp_output_file.name, key_columns_list, sort_by)

        # Создаем потоковый ответ
        def iterfile():  # Передает файл как поток
            with open(temp_output_file.name, mode="rb") as file_like:
                yield from file_like

        # Возвращаем поток с результатами
        return StreamingResponse(
            iterfile(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=comparison_result.xlsx"},
            background=BackgroundTask(lambda: os.unlink(temp_output_file.name))  # Удаляем файл после отправки
        )
    finally:
        # Удаляем входные временные файлы
        os.unlink(temp_file_before.name)
        os.unlink(temp_file_after.name)