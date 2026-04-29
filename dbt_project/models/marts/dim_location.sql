-- City dimension built from the distinct cities in stg_transactions.
-- Enriched with state, region, and tier from a static mapping.
--
-- In production this would come from a master data management system.
-- Here we derive it from the data itself + a hardcoded mapping CTE.

{{
    config(materialized='table')
}}

with city_mapping as (
    -- Static city attributes — same data as generator/config.py CITIES list
    -- Kept here so dbt owns the dimension definition
    select city, state, region, tier, is_metro from (values
        ('MUMBAI',          'Maharashtra',      'West',  'Tier 1', true),
        ('DELHI',           'Delhi',            'North', 'Tier 1', true),
        ('BENGALURU',       'Karnataka',        'South', 'Tier 1', true),
        ('HYDERABAD',       'Telangana',        'South', 'Tier 1', true),
        ('CHENNAI',         'Tamil Nadu',       'South', 'Tier 1', true),
        ('KOLKATA',         'West Bengal',      'East',  'Tier 1', true),
        ('PUNE',            'Maharashtra',      'West',  'Tier 1', true),
        ('AHMEDABAD',       'Gujarat',          'West',  'Tier 1', true),
        ('JAIPUR',          'Rajasthan',        'North', 'Tier 2', false),
        ('SURAT',           'Gujarat',          'West',  'Tier 2', false),
        ('LUCKNOW',         'Uttar Pradesh',    'North', 'Tier 2', false),
        ('KANPUR',          'Uttar Pradesh',    'North', 'Tier 2', false),
        ('NAGPUR',          'Maharashtra',      'West',  'Tier 2', false),
        ('INDORE',          'Madhya Pradesh',   'West',  'Tier 2', false),
        ('THANE',           'Maharashtra',      'West',  'Tier 2', false),
        ('BHOPAL',          'Madhya Pradesh',   'West',  'Tier 2', false),
        ('VISAKHAPATNAM',   'Andhra Pradesh',   'South', 'Tier 2', false),
        ('PATNA',           'Bihar',            'East',  'Tier 2', false),
        ('VADODARA',        'Gujarat',          'West',  'Tier 2', false),
        ('GHAZIABAD',       'Uttar Pradesh',    'North', 'Tier 2', false),
        ('LUDHIANA',        'Punjab',           'North', 'Tier 2', false),
        ('AGRA',            'Uttar Pradesh',    'North', 'Tier 2', false),
        ('NASHIK',          'Maharashtra',      'West',  'Tier 2', false),
        ('FARIDABAD',       'Haryana',          'North', 'Tier 2', false),
        ('MEERUT',          'Uttar Pradesh',    'North', 'Tier 2', false),
        ('RAJKOT',          'Gujarat',          'West',  'Tier 2', false),
        ('VARANASI',        'Uttar Pradesh',    'North', 'Tier 2', false),
        ('SRINAGAR',        'Jammu & Kashmir',  'North', 'Tier 2', false),
        ('AURANGABAD',      'Maharashtra',      'West',  'Tier 2', false),
        ('DHANBAD',         'Jharkhand',        'East',  'Tier 2', false),
        ('AMRITSAR',        'Punjab',           'North', 'Tier 2', false),
        ('NAVI MUMBAI',     'Maharashtra',      'West',  'Tier 2', false),
        ('ALLAHABAD',       'Uttar Pradesh',    'North', 'Tier 2', false),
        ('RANCHI',          'Jharkhand',        'East',  'Tier 2', false),
        ('HOWRAH',          'West Bengal',      'East',  'Tier 2', false),
        ('COIMBATORE',      'Tamil Nadu',       'South', 'Tier 2', false),
        ('JABALPUR',        'Madhya Pradesh',   'West',  'Tier 2', false),
        ('GWALIOR',         'Madhya Pradesh',   'West',  'Tier 2', false),
        ('VIJAYAWADA',      'Andhra Pradesh',   'South', 'Tier 3', false),
        ('JODHPUR',         'Rajasthan',        'North', 'Tier 3', false),
        ('MADURAI',         'Tamil Nadu',       'South', 'Tier 3', false),
        ('RAIPUR',          'Chhattisgarh',     'West',  'Tier 3', false),
        ('KOTA',            'Rajasthan',        'North', 'Tier 3', false),
        ('GUWAHATI',        'Assam',            'East',  'Tier 3', false),
        ('CHANDIGARH',      'Chandigarh',       'North', 'Tier 3', false),
        ('SOLAPUR',         'Maharashtra',      'West',  'Tier 3', false),
        ('HUBBALLI',        'Karnataka',        'South', 'Tier 3', false),
        ('TIRUCHIRAPPALLI', 'Tamil Nadu',       'South', 'Tier 3', false),
        ('BAREILLY',        'Uttar Pradesh',    'North', 'Tier 3', false),
        ('MORADABAD',       'Uttar Pradesh',    'North', 'Tier 3', false),
        ('MYSURU',          'Karnataka',        'South', 'Tier 3', false),
        ('GURGAON',         'Haryana',          'North', 'Tier 3', false),
        ('ALIGARH',         'Uttar Pradesh',    'North', 'Tier 3', false),
        ('JALANDHAR',       'Punjab',           'North', 'Tier 3', false),
        ('BHUBANESWAR',     'Odisha',           'East',  'Tier 3', false),
        ('NOIDA',           'Uttar Pradesh',    'North', 'Tier 3', false),
        ('THIRUVANANTHAPURAM', 'Kerala',        'South', 'Tier 3', false),
        ('KOCHI',           'Kerala',           'South', 'Tier 2', false),
        ('KOZHIKODE',       'Kerala',           'South', 'Tier 3', false),
        ('SALEM',           'Tamil Nadu',       'South', 'Tier 3', false),
        ('MIRA-BHAYANDAR',  'Maharashtra',      'West',  'Tier 3', false),
        ('WARANGAL',        'Telangana',        'South', 'Tier 3', false),
        ('GUNTUR',          'Andhra Pradesh',   'South', 'Tier 3', false),
        ('BHIWANDI',        'Maharashtra',      'West',  'Tier 3', false),
        ('SAHARANPUR',      'Uttar Pradesh',    'North', 'Tier 3', false),
        ('GORAKHPUR',       'Uttar Pradesh',    'North', 'Tier 3', false),
        ('BIKANER',         'Rajasthan',        'North', 'Tier 3', false),
        ('AMRAVATI',        'Maharashtra',      'West',  'Tier 3', false),
        ('JAMSHEDPUR',      'Jharkhand',        'East',  'Tier 3', false),
        ('BHILAI',          'Chhattisgarh',     'West',  'Tier 3', false),
        ('CUTTACK',         'Odisha',           'East',  'Tier 3', false),
        ('FIROZABAD',       'Uttar Pradesh',    'North', 'Tier 3', false),
        ('BHAVNAGAR',       'Gujarat',          'West',  'Tier 3', false),
        ('DEHRADUN',        'Uttarakhand',      'North', 'Tier 3', false),
        ('DURGAPUR',        'West Bengal',      'East',  'Tier 3', false),
        ('ASANSOL',         'West Bengal',      'East',  'Tier 3', false),
        ('NANDED',          'Maharashtra',      'West',  'Tier 3', false),
        ('KOLHAPUR',        'Maharashtra',      'West',  'Tier 3', false),
        ('AJMER',           'Rajasthan',        'North', 'Tier 3', false),
        ('GULBARGA',        'Karnataka',        'South', 'Tier 3', false),
        ('JAMNAGAR',        'Gujarat',          'West',  'Tier 3', false),
        ('UJJAIN',          'Madhya Pradesh',   'West',  'Tier 3', false),
        ('LONI',            'Uttar Pradesh',    'North', 'Tier 3', false),
        ('SILIGURI',        'West Bengal',      'East',  'Tier 3', false),
        ('JHANSI',          'Uttar Pradesh',    'North', 'Tier 3', false),
        ('ULHASNAGAR',      'Maharashtra',      'West',  'Tier 3', false),
        ('NELLORE',         'Andhra Pradesh',   'South', 'Tier 3', false),
        ('JAMMU',           'Jammu & Kashmir',  'North', 'Tier 3', false),
        ('SANGLI',          'Maharashtra',      'West',  'Tier 3', false),
        ('MANGALURU',       'Karnataka',        'South', 'Tier 3', false),
        ('ERODE',           'Tamil Nadu',       'South', 'Tier 3', false),
        ('BELGAUM',         'Karnataka',        'South', 'Tier 3', false),
        ('AMBATTUR',        'Tamil Nadu',       'South', 'Tier 3', false),
        ('TIRUNELVELI',     'Tamil Nadu',       'South', 'Tier 3', false),
        ('MALEGAON',        'Maharashtra',      'West',  'Tier 3', false),
        ('GAYA',            'Bihar',            'East',  'Tier 3', false),
        ('UDAIPUR',         'Rajasthan',        'North', 'Tier 3', false)
    ) as t(city, state, region, tier, is_metro)
),

-- get distinct cities actually present in the data
cities_in_data as (
    select distinct city from {{ ref('stg_transactions') }}
),

final as (
    select
        -- surrogate key using md5 hash of city name
        -- IMP: md5-based surrogate key is deterministic — the same city always gets the same key across runs.
        -- This means fact table foreign keys stay valid even if dim_location is fully rebuilt.
        {{ dbt_utils.generate_surrogate_key(['m.city']) }} as location_sk,
        m.city,
        m.state,
        m.region,
        m.tier,
        m.is_metro,
        -- population bucket derived from tier
        case m.tier
            when 'Tier 1' then 'Large (>4M)'
            when 'Tier 2' then 'Medium (1-4M)'
            else 'Small (<1M)'
        end as population_bucket
    from cities_in_data c
    inner join city_mapping m on c.city = m.city
)

select * from final