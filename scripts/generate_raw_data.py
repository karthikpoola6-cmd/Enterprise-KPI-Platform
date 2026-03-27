"""
Generate Raw Data for Enterprise KPI Platform
==============================================
Simulates messy CSV exports from 4 source systems:
  - Salesforce (orders + customers)
  - Warehouse Management (products)
  - QuickBooks (financials)
  - ADP (employees)

Intentionally includes real-world data quality issues:
  - Inconsistent date formats
  - Duplicate records (~2%)
  - Null/missing values (~3-5%)
  - Typos in region names
  - Inconsistent boolean values
"""

import csv
import random
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)

# ── Reference Data ─────────────────────────────────────────────────────

REGIONS_CLEAN = ["Dallas-Fort Worth", "Houston", "Austin", "San Antonio"]
REGIONS_MESSY = [
    "Dallas-Fort Worth", "DFW", "Dallas", "dallas-fort worth",
    "Houston", "houston", "Houston, TX",
    "Austin", "austin", "Austin, TX",
    "San Antonio", "san antonio", "San Antonio, TX"
]

STATES = {"Dallas-Fort Worth": "TX", "Houston": "TX", "Austin": "TX", "San Antonio": "TX"}

CITIES = {
    "Dallas-Fort Worth": ["Dallas", "Fort Worth", "Plano", "Irving", "Arlington", "Frisco"],
    "Houston": ["Houston", "Sugar Land", "The Woodlands", "Katy", "Pasadena"],
    "Austin": ["Austin", "Round Rock", "Cedar Park", "Georgetown", "Pflugerville"],
    "San Antonio": ["San Antonio", "New Braunfels", "Boerne", "Schertz", "Cibolo"],
}

DEPARTMENTS = ["Sales", "Operations", "Finance", "Engineering", "Human Resources", "Marketing"]

INDUSTRIES = [
    "Technology", "Healthcare", "Manufacturing", "Retail", "Financial Services",
    "Energy", "Education", "Government", "Logistics", "Professional Services"
]

SEGMENTS = ["Enterprise", "Mid-Market", "SMB"]

PRODUCT_CATEGORIES = {
    "Electronics": [
        ("Laptop Pro 15", "ELEC-LP15"), ("Desktop Workstation", "ELEC-DW01"),
        ("Monitor 27in 4K", "ELEC-MN27"), ("Wireless Keyboard", "ELEC-WK01"),
        ("USB-C Docking Station", "ELEC-DC01"), ("Webcam HD", "ELEC-WC01"),
        ("Noise-Canceling Headset", "ELEC-NC01"), ("Tablet 10in", "ELEC-TB10"),
        ("Portable SSD 1TB", "ELEC-SS01"), ("Network Switch 24-port", "ELEC-NS24"),
    ],
    "Office Supplies": [
        ("Printer Paper A4 5000pk", "OFFC-PP50"), ("Toner Cartridge Black", "OFFC-TC01"),
        ("Desk Organizer Set", "OFFC-DO01"), ("Whiteboard 6x4ft", "OFFC-WB64"),
        ("Ergonomic Chair", "OFFC-EC01"), ("Standing Desk", "OFFC-SD01"),
        ("Filing Cabinet 4-drawer", "OFFC-FC04"), ("Desk Lamp LED", "OFFC-DL01"),
        ("Notebook Pack 12", "OFFC-NB12"), ("Pen Set Premium", "OFFC-PS01"),
    ],
    "Software Licenses": [
        ("Enterprise Suite Annual", "SOFT-ES01"), ("Project Management Tool", "SOFT-PM01"),
        ("CRM Platform License", "SOFT-CR01"), ("Security Suite", "SOFT-SS01"),
        ("Cloud Storage 5TB", "SOFT-CS05"), ("Analytics Platform", "SOFT-AP01"),
        ("Communication Suite", "SOFT-CM01"), ("Design Software", "SOFT-DS01"),
    ],
    "Networking": [
        ("Firewall Enterprise", "NETW-FE01"), ("WiFi Access Point", "NETW-WA01"),
        ("Ethernet Cable Cat6 100ft", "NETW-EC01"), ("VPN Gateway", "NETW-VG01"),
        ("Server Rack 42U", "NETW-SR42"), ("UPS Battery Backup", "NETW-UPS1"),
    ],
    "Services": [
        ("IT Consulting Hour", "SERV-IC01"), ("System Installation", "SERV-SI01"),
        ("Training Workshop", "SERV-TW01"), ("Maintenance Contract", "SERV-MC01"),
        ("Data Migration Service", "SERV-DM01"), ("Security Audit", "SERV-SA01"),
    ],
}

SUPPLIERS = ["TechDist Inc", "OfficePro Supply", "CloudVendor Corp", "NetEquip LLC", "ServiceFirst Partners"]

FIRST_NAMES = [
    "James", "Maria", "Robert", "Sarah", "David", "Jennifer", "Michael", "Emily",
    "William", "Jessica", "Daniel", "Ashley", "Carlos", "Amanda", "Juan",
    "Stephanie", "Thomas", "Rachel", "Andrew", "Megan", "Christopher", "Laura",
    "Kevin", "Lisa", "Brian", "Michelle", "Ryan", "Angela", "Eric", "Catherine",
    "Jason", "Priya", "Raj", "Wei", "Chen", "Ahmed", "Fatima", "Olga",
    "Hiroshi", "Yuki", "Sofia", "Diego", "Aisha", "Marcus", "Olivia",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
    "Patel", "Kumar", "Chen", "Wang", "Kim", "Park", "Singh",
]

TITLES = {
    "Sales": ["Sales Representative", "Account Manager", "Sales Director", "Business Development Rep"],
    "Operations": ["Operations Analyst", "Logistics Coordinator", "Warehouse Manager", "Supply Chain Analyst"],
    "Finance": ["Financial Analyst", "Accountant", "Controller", "AP/AR Specialist"],
    "Engineering": ["Software Engineer", "DevOps Engineer", "Systems Architect", "QA Engineer"],
    "Human Resources": ["HR Coordinator", "Recruiter", "Benefits Specialist", "HR Manager"],
    "Marketing": ["Marketing Analyst", "Content Specialist", "Digital Marketing Manager", "Brand Coordinator"],
}

ACCOUNT_NAMES = {
    "revenue": ["Product Revenue", "Service Revenue", "License Revenue", "Consulting Revenue"],
    "expense": ["Salaries & Wages", "Rent & Facilities", "Marketing Spend", "Travel & Entertainment",
                "Software Subscriptions", "Insurance", "Utilities", "Professional Services"],
    "cogs": ["Hardware COGS", "Software COGS", "Service Delivery Cost", "Shipping & Handling"],
}

DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%b-%Y", "%Y/%m/%d"]

START_DATE = datetime(2024, 7, 1)
END_DATE = datetime(2026, 3, 15)


# ── Helper Functions ───────────────────────────────────────────────────

def random_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def messy_date(dt):
    """Return date in a random format to simulate inconsistent source data."""
    fmt = random.choice(DATE_FORMATS)
    return dt.strftime(fmt)


def maybe_null(value, null_rate=0.03):
    """Return None with a given probability to simulate missing data."""
    return None if random.random() < null_rate else value


def messy_region():
    """Return a region name with occasional typos/inconsistencies."""
    return random.choice(REGIONS_MESSY)


def clean_region():
    return random.choice(REGIONS_CLEAN)


def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_email(name):
    parts = name.lower().split()
    domain = random.choice(["riseinc.com", "rise-corp.com", "gmail.com", "outlook.com"])
    return f"{parts[0]}.{parts[1]}@{domain}"


def random_phone():
    return f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"


# ── Generate Orders ───────────────────────────────────────────────────

def generate_orders(n=2000):
    print(f"  Generating {n} orders...")
    rows = []
    order_counter = 10000

    all_products = []
    for cat, prods in PRODUCT_CATEGORIES.items():
        for name, sku in prods:
            all_products.append((name, sku, cat))

    for i in range(n):
        order_counter += 1
        order_id = f"ORD-{order_counter}"
        product = random.choice(all_products)
        order_date = random_date(START_DATE, END_DATE)
        ship_days = random.randint(1, 7)
        delivery_days = ship_days + random.randint(1, 8)
        ship_date = order_date + timedelta(days=ship_days)
        delivery_date = order_date + timedelta(days=delivery_days)

        statuses = ["completed", "completed", "completed", "completed",
                     "shipped", "shipped", "processing", "cancelled"]
        status = random.choice(statuses)
        if status == "cancelled":
            ship_date = None
            delivery_date = None
        elif status == "processing":
            ship_date = None
            delivery_date = None
        elif status == "shipped":
            delivery_date = None

        quantity = random.choices([1, 2, 3, 5, 10, 25, 50, 100],
                                  weights=[30, 25, 15, 10, 8, 5, 4, 3])[0]
        base_prices = {
            "Electronics": (150, 2500), "Office Supplies": (15, 800),
            "Software Licenses": (500, 15000), "Networking": (50, 5000),
            "Services": (200, 8000),
        }
        low, high = base_prices[product[2]]
        unit_price = round(random.uniform(low, high), 2)
        discount = random.choices([0, 5, 10, 15, 20], weights=[40, 25, 20, 10, 5])[0]

        row = {
            "order_id": order_id,
            "customer_name": maybe_null(random_name()),
            "customer_email": maybe_null(random_email(random_name())),
            "product_name": product[0],
            "product_sku": product[1],
            "category": product[2],
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_pct": discount,
            "order_date": messy_date(order_date),
            "ship_date": messy_date(ship_date) if ship_date else "",
            "delivery_date": messy_date(delivery_date) if delivery_date else "",
            "status": status,
            "region": messy_region(),
            "sales_rep": maybe_null(random_name()),
        }
        rows.append(row)

    # inject ~2% duplicate rows
    num_dupes = int(n * 0.02)
    for _ in range(num_dupes):
        rows.append(random.choice(rows).copy())
    random.shuffle(rows)

    filepath = RAW_DIR / "orders_raw.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"    -> {len(rows)} rows written to {filepath.name}")
    return rows


# ── Generate Customers ────────────────────────────────────────────────

def generate_customers(n=300):
    print(f"  Generating {n} customers...")
    rows = []
    seen_ids = set()

    for i in range(n):
        cid = f"CUST-{1000 + i}"
        name = random_name()
        region = clean_region()
        city = random.choice(CITIES[region])
        signup = random_date(datetime(2020, 1, 1), END_DATE)

        last_order = None
        if random.random() > 0.1:
            last_order = random_date(signup, END_DATE)

        row = {
            "customer_id": cid,
            "name": name,
            "email": maybe_null(random_email(name), 0.05),
            "phone": maybe_null(random_phone(), 0.08),
            "company": maybe_null(f"{random.choice(LAST_NAMES)} {random.choice(['Inc', 'LLC', 'Corp', 'Group', 'Partners'])}", 0.02),
            "industry": random.choice(INDUSTRIES),
            "segment": random.choice(SEGMENTS),
            "region": messy_region(),
            "city": city,
            "state": "TX",
            "signup_date": messy_date(signup),
            "last_order_date": messy_date(last_order) if last_order else "",
        }
        rows.append(row)
        seen_ids.add(cid)

    # inject ~2% duplicates
    num_dupes = int(n * 0.02)
    for _ in range(num_dupes):
        rows.append(random.choice(rows).copy())

    filepath = RAW_DIR / "customers_raw.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"    -> {len(rows)} rows written to {filepath.name}")
    return rows


# ── Generate Products ─────────────────────────────────────────────────

def generate_products():
    print("  Generating products...")
    rows = []
    pid = 100

    for category, products in PRODUCT_CATEGORIES.items():
        supplier = random.choice(SUPPLIERS)
        for name, sku in products:
            pid += 1
            unit_cost = round(random.uniform(10, 3000), 2)
            margin = random.uniform(0.15, 0.65)
            list_price = round(unit_cost / (1 - margin), 2)

            # messy is_active field
            active_values = ["Yes", "yes", "1", "TRUE", "True", "Y", "Active"]
            inactive_values = ["No", "no", "0", "FALSE", "False", "N", "Inactive"]
            is_active = random.choice(active_values) if random.random() > 0.1 else random.choice(inactive_values)

            subcategories = {
                "Electronics": ["Computing", "Peripherals", "Audio", "Storage", "Networking"],
                "Office Supplies": ["Paper", "Furniture", "Organization", "Writing", "Printing"],
                "Software Licenses": ["Productivity", "Security", "Storage", "Analytics", "Communication"],
                "Networking": ["Security", "Connectivity", "Infrastructure", "Power"],
                "Services": ["Consulting", "Installation", "Training", "Maintenance"],
            }

            row = {
                "product_id": f"PROD-{pid}",
                "name": name,
                "sku": sku,
                "category": category,
                "subcategory": random.choice(subcategories.get(category, ["General"])),
                "unit_cost": unit_cost,
                "list_price": list_price,
                "supplier": supplier,
                "is_active": is_active,
            }
            rows.append(row)

    filepath = RAW_DIR / "products_raw.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"    -> {len(rows)} rows written to {filepath.name}")
    return rows


# ── Generate Financials ───────────────────────────────────────────────

def generate_financials():
    print("  Generating financials...")
    rows = []

    for year in [2024, 2025, 2026]:
        months = range(1, 13) if year < 2026 else range(1, 4)
        for month in months:
            quarter = f"Q{(month - 1) // 3 + 1}"
            for dept in DEPARTMENTS:
                region = clean_region()

                # revenue lines
                for acct in ACCOUNT_NAMES["revenue"]:
                    base = random.uniform(50000, 500000)
                    # growth over time
                    growth = 1 + (year - 2024) * 0.08 + (month / 12) * 0.03
                    actual = round(base * growth * random.uniform(0.85, 1.15), 2)
                    budget = round(base * growth, 2)

                    budget_flags = ["Y", "Yes", "TRUE", "1", "true"]

                    row = {
                        "account_name": acct,
                        "account_type": "revenue",
                        "amount": actual,
                        "fiscal_year": year,
                        "fiscal_quarter": quarter,
                        "fiscal_month": month,
                        "department": dept,
                        "region": messy_region(),
                        "budget_amount": budget,
                        "is_budget": random.choice(budget_flags),
                    }
                    rows.append(row)

                # expense lines
                for acct in random.sample(ACCOUNT_NAMES["expense"], k=random.randint(3, 5)):
                    base = random.uniform(10000, 150000)
                    actual = round(base * random.uniform(0.8, 1.25), 2)
                    budget = round(base, 2)

                    row = {
                        "account_name": acct,
                        "account_type": "expense",
                        "amount": actual,
                        "fiscal_year": year,
                        "fiscal_quarter": quarter,
                        "fiscal_month": month,
                        "department": dept,
                        "region": messy_region(),
                        "budget_amount": budget,
                        "is_budget": random.choice(["Y", "Yes", "TRUE"]),
                    }
                    rows.append(row)

                # COGS lines (only for Sales and Operations)
                if dept in ["Sales", "Operations"]:
                    for acct in random.sample(ACCOUNT_NAMES["cogs"], k=random.randint(1, 3)):
                        base = random.uniform(20000, 200000)
                        actual = round(base * random.uniform(0.9, 1.1), 2)
                        budget = round(base, 2)

                        row = {
                            "account_name": acct,
                            "account_type": "cogs",
                            "amount": actual,
                            "fiscal_year": year,
                            "fiscal_quarter": quarter,
                            "fiscal_month": month,
                            "department": dept,
                            "region": messy_region(),
                            "budget_amount": budget,
                            "is_budget": random.choice(["Y", "Yes", "TRUE"]),
                        }
                        rows.append(row)

    filepath = RAW_DIR / "financials_raw.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"    -> {len(rows)} rows written to {filepath.name}")
    return rows


# ── Generate Employees ────────────────────────────────────────────────

def generate_employees(n=150):
    print(f"  Generating {n} employees...")
    rows = []

    for i in range(n):
        emp_id = f"EMP-{2000 + i}"
        name = random_name()
        dept = random.choice(DEPARTMENTS)
        title = random.choice(TITLES[dept])
        region = clean_region()
        hire_date = random_date(datetime(2018, 1, 1), datetime(2026, 1, 1))

        terminated = random.random() < 0.12
        term_date = None
        status = "Active"
        if terminated:
            term_date = random_date(hire_date + timedelta(days=90), END_DATE)
            status = "Terminated"

        if not terminated and random.random() < 0.03:
            status = "On Leave"

        salary_ranges = {
            "Sales": (55000, 120000), "Operations": (50000, 100000),
            "Finance": (60000, 130000), "Engineering": (75000, 160000),
            "Human Resources": (50000, 95000), "Marketing": (50000, 110000),
        }
        low, high = salary_ranges[dept]
        salary = round(random.uniform(low, high), 2)

        manager_names = ["Sarah Johnson", "David Martinez", "Jennifer Lee", "Michael Brown",
                         "Carlos Garcia", "Amanda Wilson"]

        row = {
            "employee_id": emp_id,
            "name": name,
            "department": dept,
            "title": title,
            "hire_date": messy_date(hire_date),
            "termination_date": messy_date(term_date) if term_date else "",
            "salary": salary,
            "region": messy_region(),
            "manager": random.choice(manager_names),
            "status": status,
        }
        rows.append(row)

    filepath = RAW_DIR / "employees_raw.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"    -> {len(rows)} rows written to {filepath.name}")
    return rows


# ── Main ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("Enterprise KPI Platform — Raw Data Generator")
    print("=" * 60)
    print(f"\nOutput directory: {RAW_DIR}\n")

    orders = generate_orders(2000)
    customers = generate_customers(300)
    products = generate_products()
    financials = generate_financials()
    employees = generate_employees(150)

    print(f"\n{'=' * 60}")
    print("Generation complete!")
    print(f"  Orders:     {len(orders)} rows")
    print(f"  Customers:  {len(customers)} rows")
    print(f"  Products:   {len(products)} rows")
    print(f"  Financials: {len(financials)} rows")
    print(f"  Employees:  {len(employees)} rows")
    print(f"\nNote: Data intentionally includes ~2% duplicates,")
    print(f"      ~3-5% null values, and inconsistent date formats")
    print(f"      to simulate real-world source system exports.")
    print(f"{'=' * 60}")
