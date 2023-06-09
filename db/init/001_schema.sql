-- Ingredients dimension table
CREATE TABLE IF NOT EXISTS dim_ingredient (
    ingredient_id SERIAL PRIMARY KEY,
    ingredient_name TEXT UNIQUE NOT NULL,
    base_unit TEXT NOT NULL DEFAULT 'oz'
);
