import os
import csv
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def build_db_url():
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )

def load_ingredients(engine):
    csv_path = "/app/data/inbox/ingredients.csv"

    with open(csv_path, newline="") as f:
        
        reader = csv.DictReader(f)

        with engine.begin() as conn:
            for row in reader:
                conn.execute(
                    text("""
                        INSERT INTO dim_ingredient (ingredient_name, base_unit)
                        VALUES (:name, :unit)
                        ON CONFLICT (ingredient_name) DO NOTHING;
                    """),
                    {
                        "name": row["ingredient_name"],
                        "unit": row["base_unit"]
                    }
                )

    print("✅ Ingredients loaded")

def main():
    engine = create_engine(build_db_url(), future=True)
    load_ingredients(engine)

if __name__ == "__main__":
    main()
