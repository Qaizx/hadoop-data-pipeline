import pandas as pd
from openpyxl import load_workbook
from openai import OpenAI
import json
import os
from dotenv import load_dotenv

load_dotenv()  # โหลดตัวแปรจาก .env

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_column_mapping_from_gpt(columns: list) -> dict:
    """ให้ GPT สร้าง mapping จากชื่อ column ไทยเป็นอังกฤษ"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": """คุณเป็น expert ในการแปลงชื่อ column จากภาษาไทยเป็นภาษาอังกฤษ

กฎ:
1. ใช้ snake_case เท่านั้น (เช่น general_expense, asset_firewall)
2. ชื่อต้องสั้น กระชับ แต่เข้าใจง่าย
3. ถ้าเป็นภาษาอังกฤษอยู่แล้ว ให้แปลงเป็น snake_case
4. ตอบเป็น JSON object เท่านั้น ไม่ต้องอธิบาย
5. format: {"ชื่อเดิม": "ชื่อใหม่", ...}"""
            },
            {
                "role": "user",
                "content": f"แปลงชื่อ columns เหล่านี้เป็นภาษาอังกฤษ:\n{json.dumps(columns, ensure_ascii=False)}"
            }
        ]
    )
    
    result = response.choices[0].message.content.strip()
    if result.startswith("```"):
        lines = result.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        result = "\n".join(lines).strip()
    
    return json.loads(result)


def get_data_mapping_from_gpt(values: list) -> dict:
    """ให้ GPT สร้าง mapping สำหรับข้อมูลในแต่ละ cell"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": """แปลงข้อมูลภาษาไทยเป็นภาษาอังกฤษ

กฎ:
1. เดือนไทย -> YYYY-MM format (ปี 66=2023, 67=2024, 68=2025)
   - ตค66 -> 2023-10, พย66 -> 2023-11, ธค66 -> 2023-12
   - มค67 -> 2024-01, กพ67 -> 2024-02, มีค67 -> 2024-03
   - เมย67 -> 2024-04, พค67 -> 2024-05, มิย67 -> 2024-06
   - กค67 -> 2024-07, สค67 -> 2024-08, กย67 -> 2024-09

2. ประเภทรายการ -> English
   - ยอดงบประมาณ -> budget
   - ยอดใช้ไป -> spent
   - ยอดคงเหลือ -> remaining

3. ข้อความอื่นๆ -> แปลเป็นภาษาอังกฤษแบบสั้นกระชับ

4. ตอบเป็น JSON object เท่านั้น: {"ค่าเดิม": "ค่าใหม่", ...}
5. ถ้าเป็นตัวเลขหรือภาษาอังกฤษอยู่แล้ว ไม่ต้องใส่ใน mapping"""
            },
            {
                "role": "user",
                "content": f"แปลงข้อมูลเหล่านี้:\n{json.dumps(values, ensure_ascii=False)}"
            }
        ]
    )
    
    result = response.choices[0].message.content.strip()
    if result.startswith("```"):
        lines = result.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        result = "\n".join(lines).strip()
    
    return json.loads(result)


def analyze_dataframe_structure(df: pd.DataFrame) -> dict:
    """ให้ GPT วิเคราะห์โครงสร้าง DataFrame และบอกว่า column ไหนคืออะไร"""
    
    # สร้าง sample data สำหรับให้ GPT วิเคราะห์
    sample_data = {}
    for col in df.columns[:10]:  # เอาแค่ 10 columns แรก
        unique_vals = df[col].dropna().unique()[:5].tolist()  # เอาแค่ 5 values
        sample_data[col] = unique_vals
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": """วิเคราะห์โครงสร้าง DataFrame และระบุ:
1. month_column: ชื่อ column ที่เก็บเดือน (ถ้ามี)
2. type_column: ชื่อ column ที่เก็บประเภท (budget/spent/remaining) (ถ้ามี)
3. type_values: ค่าที่หมายถึง budget, spent, remaining

ตอบเป็น JSON:
{
    "month_column": "ชื่อ column หรือ null",
    "type_column": "ชื่อ column หรือ null",
    "type_values": {
        "budget": "ค่าที่หมายถึง budget หรือ null",
        "spent": "ค่าที่หมายถึง spent หรือ null",
        "remaining": "ค่าที่หมายถึง remaining หรือ null"
    }
}"""
            },
            {
                "role": "user",
                "content": f"วิเคราะห์ DataFrame นี้:\nColumns: {df.columns.tolist()}\nSample data: {json.dumps(sample_data, ensure_ascii=False, default=str)}"
            }
        ]
    )
    
    result = response.choices[0].message.content.strip()
    if result.startswith("```"):
        lines = result.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        result = "\n".join(lines).strip()
    
    return json.loads(result)


def convert_excel(excel_path: str, sheet_name: str) -> pd.DataFrame:
    """แปลง Excel ที่มี merged cells เป็น DataFrame"""
    
    wb = load_workbook(excel_path, data_only=True)
    ws = wb[sheet_name]
    
    # เก็บค่าจาก merged cells
    merged_values = {}
    for merged_range in ws.merged_cells.ranges:
        top_left_value = ws.cell(merged_range.min_row, merged_range.min_col).value
        for row in range(merged_range.min_row, merged_range.max_row + 1):
            for col in range(merged_range.min_col, merged_range.max_col + 1):
                merged_values[(row, col)] = top_left_value
    
    # อ่านข้อมูลทั้งหมด
    data = []
    for row_idx, row in enumerate(ws.iter_rows(), start=1):
        row_data = []
        for col_idx, cell in enumerate(row, start=1):
            if (row_idx, col_idx) in merged_values:
                row_data.append(merged_values[(row_idx, col_idx)])
            else:
                row_data.append(cell.value)
        data.append(row_data)
    
    # สร้าง header จาก 4 rows แรก
    header_rows = data[:4]
    
    combined_headers = []
    for col_idx in range(len(header_rows[0])):
        parts = []
        for row in header_rows:
            if col_idx < len(row) and row[col_idx] is not None:
                val = str(row[col_idx]).strip().replace('\n', ' ')
                if val and val not in parts:
                    parts.append(val)
        col_name = "_".join(parts) if parts else f"col_{col_idx}"
        combined_headers.append(col_name)
    
    # ทำให้ column names unique
    seen = {}
    unique_headers = []
    for h in combined_headers:
        if h in seen:
            seen[h] += 1
            unique_headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            unique_headers.append(h)
    
    df = pd.DataFrame(data[4:], columns=unique_headers)
    
    # ===== Clean Data =====
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~df.columns.str.startswith('col_')]
    
    # ===== ให้ GPT วิเคราะห์โครงสร้างก่อน =====
    print("🤖 GPT กำลังวิเคราะห์โครงสร้าง DataFrame...")
    structure = analyze_dataframe_structure(df)
    print(f"   📌 Month column: {structure.get('month_column')}")
    print(f"   📌 Type column: {structure.get('type_column')}")
    print(f"   📌 Type values: {structure.get('type_values')}")
    
    # ===== ให้ GPT สร้าง column mapping =====
    print("\n🤖 GPT กำลังแปลงชื่อ columns...")
    column_mapping = get_column_mapping_from_gpt(df.columns.tolist())
    print(f"   ✅ สร้าง mapping สำหรับ {len(column_mapping)} columns")
    df = df.rename(columns=column_mapping)
    
    # Update structure ด้วยชื่อ column ใหม่
    if structure.get('month_column') and structure['month_column'] in column_mapping:
        structure['month_column'] = column_mapping[structure['month_column']]
    if structure.get('type_column') and structure['type_column'] in column_mapping:
        structure['type_column'] = column_mapping[structure['type_column']]
    
    # ===== ให้ GPT แปลงข้อมูลในแต่ละ cell =====
    thai_values = set()
    for col in df.columns:
        for val in df[col].dropna().unique():
            if isinstance(val, str) and any('\u0e00' <= c <= '\u0e7f' for c in val):
                thai_values.add(val)
    
    if thai_values:
        print(f"\n🤖 GPT กำลังแปลงข้อมูล {len(thai_values)} รายการ...")
        data_mapping = get_data_mapping_from_gpt(list(thai_values))
        print("   ✅ แปลงข้อมูลเรียบร้อย")
        
        for col in df.columns:
            df[col] = df[col].map(lambda x: data_mapping.get(x, x) if isinstance(x, str) else x)
        
        # Update type_values ด้วยค่าใหม่
        if structure.get('type_values'):
            for key in structure['type_values']:
                old_val = structure['type_values'][key]
                if old_val and old_val in data_mapping:
                    structure['type_values'][key] = data_mapping[old_val]
    
    # ===== Filter rows ตาม structure ที่ GPT วิเคราะห์ =====
    type_col = structure.get('type_column')
    type_vals = structure.get('type_values', {})
    
    if type_col and type_col in df.columns:
        valid_types = [v for v in type_vals.values() if v]
        if valid_types:
            print(f"\n🔍 Filtering by {type_col} in {valid_types}")
            df = df[df[type_col].isin(valid_types)].copy()
    
    # Fill month ลงมา
    month_col = structure.get('month_column')
    if month_col and month_col in df.columns:
        df[month_col] = df[month_col].ffill()
    
    df = df.reset_index(drop=True)
    
    # ===== ตรวจสอบว่ายังมีภาษาไทยเหลืออยู่ไหม =====
    remaining_thai = set()
    for col in df.columns:
        if any('\u0e00' <= c <= '\u0e7f' for c in col):
            remaining_thai.add(f"column: {col}")
        for val in df[col].dropna().unique():
            if isinstance(val, str) and any('\u0e00' <= c <= '\u0e7f' for c in val):
                remaining_thai.add(f"data: {val}")
    
    if remaining_thai:
        print(f"\n⚠️ ยังมีภาษาไทยเหลือ: {remaining_thai}")
    
    return df


# ============ Main ============
if __name__ == "__main__":
    excel_path = "data/คุมฎีกาปี2567-รายได้.xlsx"
    sheet_name = "สรุปรายเดือน 67"
    csv_path = "data/finance_2027_test.csv"
    
    print(f"📊 Converting: {excel_path}\n")
    
    df = convert_excel(excel_path, sheet_name)
    
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ Saved to {csv_path}")
    print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
    
    print("\n📋 Columns:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    print("\n📋 Preview:")
    print(df.head())