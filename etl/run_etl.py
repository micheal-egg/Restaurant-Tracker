import os
import csv
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import shutil
from pathlib import Path
from datetime import datetime



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

                valid, reason = validate_row(row)
                if not valid:
                    print("Invalid row:", row, "Reason:", reason)
                    continue  # skip bad row, keep processing

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


def load_inventory_snapshot(engine, csv_path):
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)

        with engine.begin() as conn:
            for row in reader:
                valid,reason = validate_inventory_row(row)
                if not valid:
                    print("Invalid row:", row, "Reason:", reason)
                    continue

                ingredient_id = conn.execute(
                    text("""
                         SELECT ingredient_id FROM dim_ingredient
                         WHERE ingredient_name = :name
                    """),
                    {"name": row["ingredient_name"].strip()}
                ).scalar()

                if ingredient_id is None:
                    print("Ingredient not found for row:", row)
                    continue
                conn.execute(
                    text("""
                        INSERT INTO fact_inventory_snapshot
                            (snapshot_date, ingredient_id, quantity_on_hand, unit, source_file)
                        VALUES
                            (:snapshot_date, :ingredient_id, :qty, :unit, :source_file)
                        ON CONFLICT (snapshot_date, ingredient_id) DO NOTHING;
                    """),
                    {
                        "snapshot_date": row["snapshot_date"].strip(),
                        "ingredient_id": ingredient_id,
                        "qty": float(row["quantity_on_hand"]),
                        "unit": row["unit"].strip(),
                        "source_file": Path(csv_path).name
                    }
                )

    print("Inventory snapshot loaded:", csv_path)

#Updated my Validation function
def validate_row(row):
    #Returns none if the key is missing 
    #If data is not present it returns empty string
    name = (row.get("ingredient_name") or "").strip()
    unit = (row.get("base_unit") or "").strip()

    # If either value is missing return False with message
    if not name:
        return False, "Missing ingredient_name"
    if not unit:
        return False, "Missing base_unit"

    return True, ""
def validate_inventory_row(row):
    snapshot_date = (row.get("snapshot_date") or "").strip()
    ingredient_name = (row.get("ingredient_name") or "").strip()
    unit = (row.get("unit") or "").strip()
    quantity = (row.get("quantity_on_hand") or "").strip()

    #To get stuff that is missing 

    if not snapshot_date:
        return False, "Missing snapshot_date"
    if not ingredient_name:
        return False, "Missing ingredient_name"
    if not unit:
        return False, "Missing unit"
    if not quantity:
        return False, "Missing quantity"
    
    # Validate date format
    try:
        datetime.strptime(snapshot_date, "%Y-%m-%d")
    except ValueError:
        return False, "snapshot_date must be YYYY-MM-DD"
    
    #To make sure that quntity is a number
    try:
        qty = float(quantity)
        if qty < 0:
            return False, "quantity_on_hand must be greater than or equal to 0"
    except ValueError:
        return False, "quantity_on_hand must be a number"

    return True, ""


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

    inbox_dir = Path("/app/data/inbox")

    # Process inventory snapshot files
    snapshot_files = sorted(inbox_dir.glob("inventory_snapshot_*.csv"))

    if not snapshot_files:
        print("No inventory snapshot files found in inbox.")
        return

    for csv_path in snapshot_files:
        try:
            load_inventory_snapshot(engine, str(csv_path))
            move_file(str(csv_path), "/app/data/processed")
            print("Moved to processed\n")
        except Exception:
            if csv_path.exists():
                move_file(str(csv_path), "/app/data/rejects")
                print("Moved input file to rejects\n")
            raise

if __name__ == "__main__":
    main()
