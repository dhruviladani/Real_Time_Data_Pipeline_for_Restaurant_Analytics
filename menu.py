import csv
import random
from datetime import datetime, timedelta

categories = ["Appetizers", "Main Course", "Desserts", "Beverages", "Snacks"]
item_types = {
    "Appetizers": ["Veg", "Non-Veg"],
    "Main Course": ["Veg", "Non-Veg"],
    "Desserts": ["Veg"],
    "Beverages": ["Veg"],
    "Snacks": ["Veg"]
}
item_names = {
    "Appetizers": ["Samosa", "Paneer Tikka", "Chicken Tikka", "Aloo Tikki", "Fish Fry", "Spring Rolls", "Hara Bhara Kebab", "Seekh Kebab", "Chicken Wings", "Prawn Skewers"],
    "Main Course": ["Butter Chicken", "Paneer Butter Masala", "Dal Makhani", "Chole Bhature", "Biryani", "Rogan Josh", "Palak Paneer", "Malai Kofta", "Mutton Curry", "Fish Curry"],
    "Desserts": ["Gulab Jamun", "Rasgulla", "Kheer", "Jalebi", "Kulfi", "Ras Malai", "Gajar Halwa", "Mysore Pak", "Peda", "Sandesh"],
    "Beverages": ["Masala Chai", "Lassi", "Nimbu Pani", "Cold Coffee", "Fruit Juice", "Coconut Water", "Aam Panna", "Buttermilk", "Thandai", "Falooda"],
    "Snacks": ["Pav Bhaji", "Bhel Puri", "Pani Puri", "Vada Pav", "Pakora", "Dhokla", "Kachori", "Sev Puri", "Dabeli", "Aloo Chaat"]
}
descriptions = ["Delicious and authentic {}.", "A popular Indian dish.", "Traditional Indian {} with rich flavors.", "A must-try {} from India.", "Classic {} with a twist."]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))

with open('indian_menu_data_20000.csv', 'w', newline='') as csvfile:
    fieldnames = ["MenuID", "RestaurantID", "ItemName", "Description", "Price", "Category", "Availability", "ItemType", "CreatedDate", "ModifiedDate"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for i in range(20000):
        category = random.choice(categories)
        item_name = random.choice(item_names[category])

        if (item_name in ["Chicken Tikka", "Fish Fry", "Seekh Kebab", "Chicken Wings", "Prawn Skewers", "Butter Chicken", "Biryani", "Rogan Josh", "Mutton Curry", "Fish Curry"]) : 
           item_type = "Non-Veg"

        else:
            item_type = "Veg"

       
        description = random.choice(descriptions).format(item_name)
        price = round(random.uniform(50, 500), 2)
        created_date = random_date(datetime(2023, 1, 1), datetime(2025, 2, 27))
        modified_date = created_date + timedelta(days=random.randint(0, 30))

        writer.writerow({
            "MenuID": i + 1,
            "RestaurantID": random.randint(100, 150),
            "ItemName": item_name,
            "Description": description,
            "Price": price,
            "Category": category,
            "Availability": True,
            "ItemType": item_type,
            "CreatedDate": created_date.strftime("%Y-%m-%d"),
            "ModifiedDate": modified_date.strftime("%Y-%m-%d")
        })

print("Data generation complete. Check the 'indian_menu_data_20000.csv' file.")
