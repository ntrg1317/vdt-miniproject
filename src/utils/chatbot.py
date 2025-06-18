import os

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from config.settings import settings

load_dotenv()
api_key = getattr(settings, 'AI_API_KEY', None) or os.getenv('AI_API_KEY')
client = OpenAI(
    api_key=api_key,
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
)

# Tải metadata từ database
def load_metadata(engine):
    query = """
        SELECT
            t.schema,
            t.tablename,
            t.business_term AS table_description,
            f.field,
            f.fieldtype,
            f.business_term AS field_description,
            f.is_nullable,
            f.is_primary_key,
            f.default_value
        FROM catalog.field_origin f
        JOIN catalog.table_origin t ON f.table_id = t.id
    """
    return pd.read_sql(query, engine)


def build_prompt(metadata_df, question: str) -> str:
    prompt = "Dưới đây là metadata chi tiết của các bảng trong hệ thống:\n\n"

    grouped = metadata_df.groupby(['schema', 'tablename'])
    for (schema, tablename), group in grouped:
        prompt += f"Bảng: {schema}.{tablename}\n"
        for _, row in group.iterrows():
            prompt += f"  - Cột: {row['field']} ({row['fieldtype']})"
            if row['field_description']:
                prompt += f": {row['field_description']}\n"
            else:
                prompt += ": Không có mô tả\n"
        prompt += "\n"

    prompt += f"Câu hỏi: {question}\nTrả lời bằng tiếng Việt."
    return prompt

def ask_metadata_bot(question, metadata_df):
    prompt = build_prompt(metadata_df, question)
    response = client.chat.completions.create(
        model="gemini-2.0-flash",
        messages=[
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,
    )
    return response.choices[0].message.content