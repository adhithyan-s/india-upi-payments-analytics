# All configuration for the UPI transaction generator.
# Centralised here so generate_transactions.py stays clean and all business logic is easy to find and adjust.

# ----------------------------------------------------------
# Indian cities with state, region, and tier classification
#
# Tier classification based on population and economic activity:
# Tier 1: 8 major metros (population > 4 million, high UPI adoption)
# Tier 2: 30 large cities (population 1-4 million)
# Tier 3: 62 smaller cities (population < 1 million, growing UPI usage)
#
# dim_location is built from this config.
# Every city here becomes one row in the dimension table.
# ----------------------------------------------------------
CITIES = [
    # city, state, region, tier, is_metro
    # Tier 1 — 8 major metros
    ("Mumbai",          "Maharashtra",      "West",  "Tier 1", True),
    ("Delhi",           "Delhi",            "North", "Tier 1", True),
    ("Bengaluru",       "Karnataka",        "South", "Tier 1", True),
    ("Hyderabad",       "Telangana",        "South", "Tier 1", True),
    ("Chennai",         "Tamil Nadu",       "South", "Tier 1", True),
    ("Kolkata",         "West Bengal",      "East",  "Tier 1", True),
    ("Pune",            "Maharashtra",      "West",  "Tier 1", True),
    ("Ahmedabad",       "Gujarat",          "West",  "Tier 1", True),

    # Tier 2 — large cities
    ("Kochi",           "Kerala",           "South", "Tier 2", False),
    ("Jaipur",          "Rajasthan",        "North", "Tier 2", False),
    ("Surat",           "Gujarat",          "West",  "Tier 2", False),
    ("Lucknow",         "Uttar Pradesh",    "North", "Tier 2", False),
    ("Kanpur",          "Uttar Pradesh",    "North", "Tier 2", False),
    ("Nagpur",          "Maharashtra",      "West",  "Tier 2", False),
    ("Indore",          "Madhya Pradesh",   "West",  "Tier 2", False),
    ("Thane",           "Maharashtra",      "West",  "Tier 2", False),
    ("Bhopal",          "Madhya Pradesh",   "West",  "Tier 2", False),
    ("Visakhapatnam",   "Andhra Pradesh",   "South", "Tier 2", False),
    ("Patna",           "Bihar",            "East",  "Tier 2", False),
    ("Vadodara",        "Gujarat",          "West",  "Tier 2", False),
    ("Ghaziabad",       "Uttar Pradesh",    "North", "Tier 2", False),
    ("Ludhiana",        "Punjab",           "North", "Tier 2", False),
    ("Agra",            "Uttar Pradesh",    "North", "Tier 2", False),
    ("Nashik",          "Maharashtra",      "West",  "Tier 2", False),
    ("Faridabad",       "Haryana",          "North", "Tier 2", False),
    ("Meerut",          "Uttar Pradesh",    "North", "Tier 2", False),
    ("Rajkot",          "Gujarat",          "West",  "Tier 2", False),
    ("Varanasi",        "Uttar Pradesh",    "North", "Tier 2", False),
    ("Srinagar",        "Jammu & Kashmir",  "North", "Tier 2", False),
    ("Aurangabad",      "Maharashtra",      "West",  "Tier 2", False),
    ("Dhanbad",         "Jharkhand",        "East",  "Tier 2", False),
    ("Amritsar",        "Punjab",           "North", "Tier 2", False),
    ("Navi Mumbai",     "Maharashtra",      "West",  "Tier 2", False),
    ("Allahabad",       "Uttar Pradesh",    "North", "Tier 2", False),
    ("Ranchi",          "Jharkhand",        "East",  "Tier 2", False),
    ("Howrah",          "West Bengal",      "East",  "Tier 2", False),
    ("Coimbatore",      "Tamil Nadu",       "South", "Tier 2", False),
    ("Jabalpur",        "Madhya Pradesh",   "West",  "Tier 2", False),
    ("Gwalior",         "Madhya Pradesh",   "West",  "Tier 2", False),

    # Tier 3 — smaller cities (growing UPI adoption)
    ("Vijayawada",      "Andhra Pradesh",   "South", "Tier 3", False),
    ("Jodhpur",         "Rajasthan",        "North", "Tier 3", False),
    ("Madurai",         "Tamil Nadu",       "South", "Tier 3", False),
    ("Raipur",          "Chhattisgarh",     "West",  "Tier 3", False),
    ("Kota",            "Rajasthan",        "North", "Tier 3", False),
    ("Guwahati",        "Assam",            "East",  "Tier 3", False),
    ("Chandigarh",      "Chandigarh",       "North", "Tier 3", False),
    ("Solapur",         "Maharashtra",      "West",  "Tier 3", False),
    ("Hubballi",        "Karnataka",        "South", "Tier 3", False),
    ("Tiruchirappalli", "Tamil Nadu",       "South", "Tier 3", False),
    ("Bareilly",        "Uttar Pradesh",    "North", "Tier 3", False),
    ("Moradabad",       "Uttar Pradesh",    "North", "Tier 3", False),
    ("Mysuru",          "Karnataka",        "South", "Tier 3", False),
    ("Gurgaon",         "Haryana",          "North", "Tier 3", False),
    ("Aligarh",         "Uttar Pradesh",    "North", "Tier 3", False),
    ("Jalandhar",       "Punjab",           "North", "Tier 3", False),
    ("Bhubaneswar",     "Odisha",           "East",  "Tier 3", False),
    ("Noida",           "Uttar Pradesh",    "North", "Tier 3", False),
    ("Thiruvananthapuram", "Kerala",        "South", "Tier 3", False),
    ("Kozhikode",       "Kerala",           "South", "Tier 3", False),
    ("Salem",           "Tamil Nadu",       "South", "Tier 3", False),
    ("Mira-Bhayandar",  "Maharashtra",      "West",  "Tier 3", False),
    ("Warangal",        "Telangana",        "South", "Tier 3", False),
    ("Guntur",          "Andhra Pradesh",   "South", "Tier 3", False),
    ("Bhiwandi",        "Maharashtra",      "West",  "Tier 3", False),
    ("Saharanpur",      "Uttar Pradesh",    "North", "Tier 3", False),
    ("Gorakhpur",       "Uttar Pradesh",    "North", "Tier 3", False),
    ("Bikaner",         "Rajasthan",        "North", "Tier 3", False),
    ("Amravati",        "Maharashtra",      "West",  "Tier 3", False),
    ("Noida",           "Uttar Pradesh",    "North", "Tier 3", False),
    ("Jamshedpur",      "Jharkhand",        "East",  "Tier 3", False),
    ("Bhilai",          "Chhattisgarh",     "West",  "Tier 3", False),
    ("Cuttack",         "Odisha",           "East",  "Tier 3", False),
    ("Firozabad",       "Uttar Pradesh",    "North", "Tier 3", False),
    ("Kochi",           "Kerala",           "South", "Tier 3", False),
    ("Bhavnagar",       "Gujarat",          "West",  "Tier 3", False),
    ("Dehradun",        "Uttarakhand",      "North", "Tier 3", False),
    ("Durgapur",        "West Bengal",      "East",  "Tier 3", False),
    ("Asansol",         "West Bengal",      "East",  "Tier 3", False),
    ("Nanded",          "Maharashtra",      "West",  "Tier 3", False),
    ("Kolhapur",        "Maharashtra",      "West",  "Tier 3", False),
    ("Ajmer",           "Rajasthan",        "North", "Tier 3", False),
    ("Gulbarga",        "Karnataka",        "South", "Tier 3", False),
    ("Jamnagar",        "Gujarat",          "West",  "Tier 3", False),
    ("Ujjain",          "Madhya Pradesh",   "West",  "Tier 3", False),
    ("Loni",            "Uttar Pradesh",    "North", "Tier 3", False),
    ("Siliguri",        "West Bengal",      "East",  "Tier 3", False),
    ("Jhansi",          "Uttar Pradesh",    "North", "Tier 3", False),
    ("Ulhasnagar",      "Maharashtra",      "West",  "Tier 3", False),
    ("Nellore",         "Andhra Pradesh",   "South", "Tier 3", False),
    ("Jammu",           "Jammu & Kashmir",  "North", "Tier 3", False),
    ("Sangli",          "Maharashtra",      "West",  "Tier 3", False),
    ("Mangaluru",       "Karnataka",        "South", "Tier 3", False),
    ("Erode",           "Tamil Nadu",       "South", "Tier 3", False),
    ("Belgaum",         "Karnataka",        "South", "Tier 3", False),
    ("Ambattur",        "Tamil Nadu",       "South", "Tier 3", False),
    ("Tirunelveli",     "Tamil Nadu",       "South", "Tier 3", False),
    ("Malegaon",        "Maharashtra",      "West",  "Tier 3", False),
    ("Gaya",            "Bihar",            "East",  "Tier 3", False),
    ("Udaipur",         "Rajasthan",        "North", "Tier 3", False),
]

# ----------------------------------------------------------
# City transaction volume weights
# Tier 1 cities generate far more UPI volume than Tier 3. 
# These weights reflect real NPCI data where metros account for ~65% of total UPI transaction value.
# Weights don't need to sum to 1 — numpy normalises them.
# ----------------------------------------------------------
def get_city_weights(cities):
    weights = []
    for _, _, _, tier, is_metro in cities:
        if is_metro:
            weights.append(12.0)   # metros: highest volume
        elif tier == "Tier 1":
            weights.append(8.0)
        elif tier == "Tier 2":
            weights.append(3.0)
        else:
            weights.append(1.0)    # Tier 3: lower but growing
    return weights


# ----------------------------------------------------------
# UPI Apps with market share weights
# Source: NPCI published data + industry reports 2024
#
# dim_upi_app has 5 rows — one per app.
# Weights here determine how often each app appears in fact_transactions, reflecting real market share.
# ----------------------------------------------------------
UPI_APPS = [
    # app_name,       company,          market_share_pct, weight
    ("PhonePe",       "Walmart",         48.0,            48),
    ("Google Pay",    "Google",          37.0,            37),
    ("Paytm",         "One97 Comms",      8.0,             8),
    ("BHIM",          "NPCI",             4.0,             4),
    ("Amazon Pay",    "Amazon",           3.0,             3),
]

# ----------------------------------------------------------
# Merchant categories with transaction characteristics
#
# Each category has:
# - category_code: used as the key in dim_merchant
# - category_name: human-readable label
# - category_group: higher-level grouping
# - avg_amount_inr: mean transaction amount
# - std_amount_inr: standard deviation (higher = more spread)
# - weight: how often this category appears in transactions
# - failure_rate: fraction of transactions that fail
# - typical_hours: peak hours for this category (24h)
#---------------------------------------------------------------
MERCHANT_CATEGORIES = [
    {
        "category_code":  "food_delivery",
        "category_name":  "Food Delivery",
        "category_group": "Lifestyle",
        "avg_amount_inr": 380,
        "std_amount_inr": 180,
        "min_amount":     49,
        "max_amount":     2500,
        "weight":         22,          # most common UPI use case
        "failure_rate":   0.04,
        "peak_hours":     [12, 13, 19, 20, 21],
    },
    {
        "category_code":  "retail",
        "category_name":  "Retail Shopping",
        "category_group": "Lifestyle",
        "avg_amount_inr": 850,
        "std_amount_inr": 600,
        "min_amount":     50,
        "max_amount":     15000,
        "weight":         18,
        "failure_rate":   0.03,
        "peak_hours":     [11, 12, 16, 17, 18, 19],
    },
    {
        "category_code":  "utilities",
        "category_name":  "Utilities & Bills",
        "category_group": "Essential",
        "avg_amount_inr": 1200,
        "std_amount_inr": 800,
        "min_amount":     100,
        "max_amount":     8000,
        "weight":         14,
        "failure_rate":   0.05,        # higher failure — bank timeouts
        "peak_hours":     list(range(9, 18)),
    },
    {
        "category_code":  "travel",
        "category_name":  "Travel & Transport",
        "category_group": "Travel",
        "avg_amount_inr": 2200,
        "std_amount_inr": 1800,
        "min_amount":     50,
        "max_amount":     25000,
        "weight":         10,
        "failure_rate":   0.06,
        "peak_hours":     [7, 8, 17, 18, 19],
    },
    {
        "category_code":  "recharge",
        "category_name":  "Mobile Recharge",
        "category_group": "Essential",
        "avg_amount_inr": 299,
        "std_amount_inr": 150,
        "min_amount":     19,
        "max_amount":     1499,
        "weight":         12,
        "failure_rate":   0.03,
        "peak_hours":     list(range(8, 22)),
    },
    {
        "category_code":  "p2p_transfer",
        "category_name":  "P2P Transfer",
        "category_group": "Transfer",
        "avg_amount_inr": 3500,
        "std_amount_inr": 4000,
        "min_amount":     1,
        "max_amount":     100000,
        "weight":         15,          # very common — splitting bills, etc.
        "failure_rate":   0.04,
        "peak_hours":     list(range(9, 23)),
    },
    {
        "category_code":  "education",
        "category_name":  "Education",
        "category_group": "Essential",
        "avg_amount_inr": 4500,
        "std_amount_inr": 3000,
        "min_amount":     500,
        "max_amount":     50000,
        "weight":         4,
        "failure_rate":   0.04,
        "peak_hours":     [9, 10, 11, 14, 15],
    },
    {
        "category_code":  "healthcare",
        "category_name":  "Healthcare",
        "category_group": "Essential",
        "avg_amount_inr": 1800,
        "std_amount_inr": 2000,
        "min_amount":     100,
        "max_amount":     30000,
        "weight":         5,
        "failure_rate":   0.03,
        "peak_hours":     [9, 10, 11, 16, 17],
    },
    {
        "category_code":  "entertainment",
        "category_name":  "Entertainment",
        "category_group": "Lifestyle",
        "avg_amount_inr": 450,
        "std_amount_inr": 300,
        "min_amount":     99,
        "max_amount":     5000,
        "weight":         6,
        "failure_rate":   0.04,
        "peak_hours":     [18, 19, 20, 21, 22],
    },
    {
        "category_code":  "grocery",
        "category_name":  "Grocery",
        "category_group": "Essential",
        "avg_amount_inr": 650,
        "std_amount_inr": 400,
        "min_amount":     50,
        "max_amount":     5000,
        "weight":         10,
        "failure_rate":   0.03,
        "peak_hours":     [8, 9, 17, 18, 19, 20],
    },
    {
        "category_code":  "fuel",
        "category_name":  "Fuel & Petrol",
        "category_group": "Essential",
        "avg_amount_inr": 1100,
        "std_amount_inr": 500,
        "min_amount":     100,
        "max_amount":     6000,
        "weight":         5,
        "failure_rate":   0.04,
        "peak_hours":     [7, 8, 9, 17, 18, 19],
    },
    {
        "category_code":  "insurance",
        "category_name":  "Insurance",
        "category_group": "Financial",
        "avg_amount_inr": 8000,
        "std_amount_inr": 6000,
        "min_amount":     500,
        "max_amount":     75000,
        "weight":         3,
        "failure_rate":   0.05,
        "peak_hours":     [10, 11, 14, 15, 16],
    },
    {
        "category_code":  "investment",
        "category_name":  "Investment & Mutual Funds",
        "category_group": "Financial",
        "avg_amount_inr": 5000,
        "std_amount_inr": 8000,
        "min_amount":     100,
        "max_amount":     200000,
        "weight":         3,
        "failure_rate":   0.03,
        "peak_hours":     [9, 10, 14, 15],
    },
    {
        "category_code":  "gaming",
        "category_name":  "Gaming & Esports",
        "category_group": "Lifestyle",
        "avg_amount_inr": 299,
        "std_amount_inr": 400,
        "min_amount":     9,
        "max_amount":     5000,
        "weight":         3,
        "failure_rate":   0.05,
        "peak_hours":     [20, 21, 22, 23],
    },
    {
        "category_code":  "govt_services",
        "category_name":  "Government Services",
        "category_group": "Essential",
        "avg_amount_inr": 750,
        "std_amount_inr": 600,
        "min_amount":     50,
        "max_amount":     10000,
        "weight":         4,
        "failure_rate":   0.07,        # govt portals have higher failure rate
        "peak_hours":     [10, 11, 12, 14, 15],
    },
]

# ----------------------------------------------------------
# Payment types
# dim_payment_type — 4 rows only.
# P2M dominates because most UPI transactions are merchant payments.
# ----------------------------------------------------------
PAYMENT_TYPES = [
    # type_code,     type_name,                weight
    ("p2m",          "Peer to Merchant",        55),
    ("p2p",          "Peer to Peer",            30),
    ("bill_payment", "Bill Payment",            10),
    ("recharge",     "Mobile Recharge",          5),
]

# ----------------------------------------------------------
# Transaction status probabilities
# Overall ~95% success rate — calibrated to NPCI reported figures
# ----------------------------------------------------------
STATUSES = [
    ("SUCCESS", 0.945),
    ("FAILED",  0.045),
    ("PENDING", 0.010),
]

# ----------------------------------------------------------
# Failure reasons (only for FAILED transactions)
# ----------------------------------------------------------
FAILURE_REASONS = [
    ("Insufficient funds",              0.35),
    ("Bank server timeout",             0.25),
    ("Invalid UPI PIN",                 0.15),
    ("Transaction limit exceeded",      0.10),
    ("Receiver UPI ID not found",       0.08),
    ("Network error",                   0.05),
    ("Duplicate transaction declined",  0.02),
]

# ----------------------------------------------------------
# Device types
# ----------------------------------------------------------
DEVICE_TYPES = [
    ("Android",  0.72),
    ("iOS",      0.23),
    ("Web",      0.05),
]

# ----------------------------------------------------------
# Indian festival dates — used for is_festival_day in dim_date
# UPI shows massive spikes on festival days.
# Diwali 2023 set the then-record for daily UPI volume.
# This makes the dim_date uniquely Indian
# ----------------------------------------------------------
FESTIVAL_DATES = {
    # 2023
    "2023-01-14": "Makar Sankranti",
    "2023-01-26": "Republic Day",
    "2023-03-08": "Holi",
    "2023-03-30": "Ram Navami",
    "2023-04-07": "Good Friday",
    "2023-04-14": "Ambedkar Jayanti",
    "2023-04-22": "Eid ul-Fitr",
    "2023-06-29": "Eid ul-Adha",
    "2023-08-15": "Independence Day",
    "2023-08-30": "Raksha Bandhan",
    "2023-09-07": "Janmashtami",
    "2023-09-19": "Ganesh Chaturthi",
    "2023-10-02": "Gandhi Jayanti",
    "2023-10-24": "Dussehra",
    "2023-11-12": "Diwali",
    "2023-11-13": "Diwali (Day 2)",
    "2023-11-27": "Guru Nanak Jayanti",
    "2023-12-25": "Christmas",
    "2023-12-31": "New Year Eve",
    # 2024
    "2024-01-01": "New Year",
    "2024-01-14": "Makar Sankranti",
    "2024-01-22": "Ram Mandir Consecration",  # historic UPI spike day
    "2024-01-26": "Republic Day",
    "2024-03-25": "Holi",
    "2024-04-09": "Ram Navami",
    "2024-04-11": "Eid ul-Fitr",
    "2024-04-14": "Ambedkar Jayanti / Tamil New Year",
    "2024-04-17": "Good Friday",
    "2024-06-17": "Eid ul-Adha",
    "2024-07-17": "Muharram",
    "2024-08-15": "Independence Day",
    "2024-08-19": "Raksha Bandhan",
    "2024-08-26": "Janmashtami",
    "2024-09-07": "Ganesh Chaturthi",
    "2024-10-02": "Gandhi Jayanti",
    "2024-10-12": "Dussehra",
    "2024-10-31": "Diwali",
    "2024-11-01": "Diwali (Day 2)",
    "2024-11-15": "Guru Nanak Jayanti",
    "2024-12-25": "Christmas",
    "2024-12-31": "New Year Eve",
}

# ----------------------------------------------------------
# Hour-of-day transaction volume weights
# UPI peaks at lunch (12-14) and evening (19-22)
# Almost no transactions between 2am-5am
# ----------------------------------------------------------
HOUR_WEIGHTS = [
    0.5,   # 0  midnight
    0.3,   # 1
    0.2,   # 2
    0.15,  # 3
    0.15,  # 4
    0.3,   # 5
    0.8,   # 6
    1.5,   # 7  morning commute
    2.5,   # 8
    3.0,   # 9  work hours begin
    3.2,   # 10
    3.5,   # 11
    4.5,   # 12 lunch peak
    4.8,   # 13
    3.8,   # 14
    3.2,   # 15
    3.0,   # 16
    3.5,   # 17 evening commute
    4.0,   # 18
    4.8,   # 19 dinner + shopping peak
    5.0,   # 20 highest volume hour
    4.5,   # 21
    3.5,   # 22
    1.5,   # 23
]

# ----------------------------------------------------------
# Weekend multiplier — UPI volumes are ~20% higher on weekends
# due to shopping, dining out, and travel
# ----------------------------------------------------------
WEEKEND_MULTIPLIER = 1.20

# ----------------------------------------------------------
# Festival day multiplier — UPI volume 2-3x on major festivals
# Diwali 2023 recorded 430 million transactions in a single day
# ----------------------------------------------------------
FESTIVAL_MULTIPLIER = 2.50