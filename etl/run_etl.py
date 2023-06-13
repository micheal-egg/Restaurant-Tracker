import os
import csv
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import shutil
from pathlib import Path


load_dotenv()

# To build my database URL
def build_db_url():
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )

def load_ingredients(engine, csv_path):
    
    # With makes sure I close my file after reading
    with open(csv_path, newline="") as f:

        # This reads the CSV into a dictionary format
        reader = csv.DictReader(f)

        # Create engine connection to speak to Database
        with engine.begin() as conn:
            for row in reader:
                conn.execute(
                    # For SQL inserts with conflict handling
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

    print("Ingredients loaded")

# Validate row data
def row_valid(CSV_path):

    with open(CSV_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["ingredient_name"] == "" or row["base_unit"] == "":
                print ("Invalid row found:", row)
                return False
            else:
                print("Row is valid")

    return True

# Move file from inbox to processed
def move_file(src_path, dest_dir):
    # This will turn my string path into a Path object
    src = Path(src_path)
    # This joins the dest_dir with the filename from src
    dest = Path(dest_dir) / src.name
    # Create dest directory if it doesn't exist
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Move the file
    shutil.move(str(src), str(dest))



def main():
    engine = create_engine(build_db_url(), future=True)
    csv_path = "/app/data/inbox/ingredients.csv"

    # Error handling to move files appropriately
    try:
        load_ingredients(engine)
        move_file(csv_path, "/app/data/processed")
        print("Moved to processed")
    # Catch Exceptions and store the error event 
    except Exception as e:
        # Only try to move if the file exists
        if Path(csv_path).exists():
            move_file(csv_path, "/app/data/rejects")
            print("Moved to rejects")
        raise

if __name__ == "__main__":
    main()
