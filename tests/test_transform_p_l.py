from src.transform_lambda import transform_staff
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
