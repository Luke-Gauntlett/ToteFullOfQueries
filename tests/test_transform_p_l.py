from src.transform_lambda import transform_staff,transform_location,create_date_table
import pandas as pd

def test_transform_staff():
    sample_staff = [
    {
        "staff_id": 1,
        "first_name": "Jeremie",
        "last_name": "Franey",
        "department_id": 1,
        "email_address": "jeremie.franey@terrifictotes.com",
        "created_at": "2022-11-03 14:20:51.563000",
        "last_updated": "2022-11-03 14:20:51.563000"
    },
    {
        "staff_id": 2,
        "first_name": "Deron",
        "last_name": "Beier",
        "department_id": 2,
        "email_address": "deron.beier@terrifictotes.com",
        "created_at": "2022-11-03 14:20:51.563000",
        "last_updated": "2022-11-03 14:20:51.563000"
    },
    {
        "staff_id": 3,
        "first_name": "Jeanette",
        "last_name": "Erdman",
        "department_id": 3,
        "email_address": "jeanette.erdman@terrifictotes.com",
        "created_at": "2022-11-03 14:20:51.563000",
        "last_updated": "2022-11-03 14:20:51.563000"
    }]

    sample_department = [
    {
        "department_id": 1,
        "department_name": "Sales",
        "location": "Manchester",
        "manager": "Richard Roma",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    },
    {
        "department_id": 2,
        "department_name": "Purchasing",
        "location": "Manchester",
        "manager": "Naomi Lapaglia",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    },
    {
        "department_id": 3,
        "department_name": "Production",
        "location": "Leeds",
        "manager": "Chester Ming",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    }]

    result = transform_staff(sample_staff, sample_department)

   
    expected_df_first_entry = pd.DataFrame([{"staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_name": "Sales",
            "location" : "Manchester",
            "email_address": "jeremie.franey@terrifictotes.com"},

            {"staff_id": 2,
            "first_name": "Deron",
            "last_name": "Beier",
            "department_name": "Purchasing",
            "location" : "Manchester",
            "email_address": "deron.beier@terrifictotes.com"},

            {"staff_id": 3,
            "first_name": "Jeanette",
            "last_name": "Erdman",
            "department_name": "Production",
            "location" : "Leeds",
            "email_address": "jeanette.erdman@terrifictotes.com"}])   
    
    expected_df_first_entry.set_index("staff_id", inplace=True)
    
    # print(expected_df_first_entry.to_string())
    # print(result.to_string()) 
    
    pd.testing.assert_frame_equal(expected_df_first_entry, result)


def test_transform_staff_empty_input():     
    result = transform_staff([], [])        
    assert result.empty


def test_location():

    sample_addresses = [
    {
        "address_id": 1,
        "address_line_1": "6826 Herzog Via",
        "address_line_2": None,
        "district": "Avon",
        "city": "New Patienceburgh",
        "postal_code": "28441",
        "country": "Turkey",
        "phone": "1803 637401",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    },
    {
        "address_id": 2,
        "address_line_1": "179 Alexie Cliffs",
        "address_line_2": None,
        "district": None,
        "city": "Aliso Viejo",
        "postal_code": "99305-7380",
        "country": "San Marino",
        "phone": "9621 880720",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    },
    {
        "address_id": 3,
        "address_line_1": "148 Sincere Fort",
        "address_line_2": None,
        "district": None,
        "city": "Lake Charles",
        "postal_code": "89360",
        "country": "Samoa",
        "phone": "0730 783349",
        "created_at": "2022-11-03 14:20:49.962000",
        "last_updated": "2022-11-03 14:20:49.962000"
    }]

    expected_location = [
    {
        "location_id": 1,
        "address_line_1": "6826 Herzog Via",
        "address_line_2": None,
        "district": "Avon",
        "city": "New Patienceburgh",
        "postal_code": "28441",
        "country": "Turkey",
        "phone": "1803 637401"
    },
    {
        "location_id": 2,
        "address_line_1": "179 Alexie Cliffs",
        "address_line_2": None,
        "district": None,
        "city": "Aliso Viejo",
        "postal_code": "99305-7380",
        "country": "San Marino",
        "phone": "9621 880720"
    },
    {
        "location_id": 3,
        "address_line_1": "148 Sincere Fort",
        "address_line_2": None,
        "district": None,
        "city": "Lake Charles",
        "postal_code": "89360",
        "country": "Samoa",
        "phone": "0730 783349"
    }]

    result = transform_location(sample_addresses)

    expected_df = pd.DataFrame(expected_location)

    expected_df.set_index("location_id", inplace=True)

    pd.testing.assert_frame_equal(expected_df, result)



def test_make_a_date():
    
    result = create_date_table()

    date_as_datetime = pd.to_datetime("2025-03-04")

    expected = pd.DataFrame({"date":date_as_datetime,"year":2025,"month":3,"day":4,"day_of_week":2,"day_name":"Tuesday","month_name":"March","quarter":1},index=[0])
    
    expected.set_index("date", inplace=True)

    row_expected = expected.loc['2025-03-04'] # make it a series

    row = result.loc['2025-03-04'] # get single row of dataframe 


    pd.testing.assert_series_equal(row, row_expected)
