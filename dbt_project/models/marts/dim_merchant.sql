-- Merchant category dimension. Small table — 15 rows.
-- Fully static — built from hardcoded values matching config.py

{{
    config(materialized='table')
}}

with merchant_categories as (
    select category_code, category_name, category_group, is_essential
    from (values
        ('food_delivery', 'Food Delivery',             'Lifestyle',  false),
        ('retail',        'Retail Shopping',           'Lifestyle',  false),
        ('utilities',     'Utilities & Bills',         'Essential',  true),
        ('travel',        'Travel & Transport',        'Travel',     false),
        ('recharge',      'Mobile Recharge',           'Essential',  true),
        ('p2p_transfer',  'P2P Transfer',              'Transfer',   false),
        ('education',     'Education',                 'Essential',  true),
        ('healthcare',    'Healthcare',                'Essential',  true),
        ('entertainment', 'Entertainment',             'Lifestyle',  false),
        ('grocery',       'Grocery',                   'Essential',  true),
        ('fuel',          'Fuel & Petrol',             'Essential',  true),
        ('insurance',     'Insurance',                 'Financial',  false),
        ('investment',    'Investment & Mutual Funds', 'Financial',  false),
        ('gaming',        'Gaming & Esports',          'Lifestyle',  false),
        ('govt_services', 'Government Services',       'Essential',  true)
    ) as t(category_code, category_name, category_group, is_essential)
)

select
    {{ dbt_utils.generate_surrogate_key(['category_code']) }} as merchant_sk,
    category_code,
    category_name,
    category_group,
    is_essential
from merchant_categories