# from src.load_lambda import read_parquet, load_df_to_warehouse, connect_to_warehouse
# from src.transform_lambda import transform_location
# import boto3
# from moto import mock_aws
# import pandas as pd
# import pytest
# import os
# import sqlite3 # import create_engine
# from pprint import pprint
# import pg8000

# location_data = [
#     {
#         "address_id": 1,
#         "address_line_1": "6826 Herzog Via",
#         "address_line_2": None,
#         "district": "Avon",
#         "city": "New Patienceburgh",
#         "postal_code": "28441",
#         "country": "Turkey",
#         "phone": "1803 637401",
#         "created_at": "2022-11-03 14:20:49.962000",
#         "last_updated": "2022-11-03 14:20:49.962000"
#     },
#     {
#         "address_id": 2,
#         "address_line_1": "179 Alexie Cliffs",
#         "address_line_2": None,
#         "district": None,
#         "city": "Aliso Viejo",
#         "postal_code": "99305-7380",
#         "country": "San Marino",
#         "phone": "9621 880720",
#         "created_at": "2022-11-03 14:20:49.962000",
#         "last_updated": "2022-11-03 14:20:49.962000"
#     },
#     {
#         "address_id": 3,
#         "address_line_1": "148 Sincere Fort",
#         "address_line_2": None,
#         "district": None,
#         "city": "Lake Charles",
#         "postal_code": "89360",
#         "country": "Samoa",
#         "phone": "0730 783349",
#         "created_at": "2022-11-03 14:20:49.962000",
#         "last_updated": "2022-11-03 14:20:49.962000"
#     }]

# @pytest.fixture(scope="function", autouse=True)
# def aws_credentials():
#     """Mocked AWS Credentials for moto"""
#     os.environ["AWS_ACCESS_KEY_ID"] = "test"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
#     os.environ["AWS_SECURITY_TOKEN"] = "test"
#     os.environ["AWS_SESSION_TOKEN"] = "test"
#     os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

# @pytest.fixture
# def mock_s3_client_read():
#     """Fixture to mock the S3 client and prepopulate with test data."""
#     with mock_aws():
#         client = boto3.client("s3", region_name="eu-west-2")

#         bucket_name = "test-bucket"
#         file_paths = ["data/by_time/2025/March/03/14:24:54.932025/address.parquet"]
#         file_name = file_paths[0].split("/")[-1].split(".")[0]

#         client.create_bucket(
#             Bucket=bucket_name,
#             CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#         )
#         transformed_data = transform_location(location_data)
#         parquet_data = transformed_data.to_parquet()
#         client.put_object(Bucket=bucket_name, Key=file_paths[0], Body=parquet_data)

#         yield client, bucket_name, file_paths, file_name


# class TestReadParquet: 
#     def test_transformed_parquet_file_into_data_frame(self, mock_s3_client_read):
#         """Test reading a single parquet file from S3."""
#         client, bucket_name, file_paths, file_name = mock_s3_client_read

#         result = read_parquet(file_paths, client, bucket_name)

#         assert isinstance(result[file_name], pd.DataFrame)

#     def test_data_is_correctly_indexed(self, mock_s3_client_read):
#         client, bucket_name, file_paths, file_name = mock_s3_client_read

#         result = read_parquet(file_paths, client, bucket_name)

#         assert list(result[file_name].columns) == ["address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]

#     def test_data_is_inputted_correctly(self, mock_s3_client_read):
#         client, bucket_name, file_paths, file_name = mock_s3_client_read

#         result = read_parquet(file_paths, client, bucket_name)

#         assert result[file_name].iloc[0]["district"] == "Avon"
#         assert result[file_name].iloc[1]["address_line_1"] == "179 Alexie Cliffs"
#         assert result[file_name].iloc[2]["city"] == "Lake Charles"

#     def test_na_values_are_inputted_correctly(self, mock_s3_client_read):
#         client, bucket_name, file_paths, file_name = mock_s3_client_read

#         result = read_parquet(file_paths, client, bucket_name)

#         assert pd.isna(result[file_name].iloc[1]["district"])
#         assert pd.isna(result[file_name].iloc[2]["district"])


# @pytest.fixture
# def temp_db():
#     """Creates a temporary in-memory SQLite database."""
#     # engine = create_engine("sqlite:///:memory:")  
#     # yield engine.connect()  
#     yield sqlite3.connect("test.db")
#     # engine.dispose()  

# class TestWarehouse:
#     def test_data_written_to_warehouse(self, temp_db):
#         test_df = pd.DataFrame(
#             [
#                 {
#                     "design_id": 1,
#                     "created_at": "2022",
#                     "design_name": "Wooden"            
#                 },
#                 {
#                     "design_id": 2,
#                     "created_at": "2023",
#                     "design_name": "Bronze"                    
#                 },
#                 {
#                     "design_id": 3,
#                     "created_at": "2023",
#                     "design_name": "Bronze"                   
#                 },
#             ]
#         )
#         # test_df.reset_index(drop=True, inplace=True)
#         test_df.set_index('design_id',inplace=True)
        
#         engine_conn = temp_db
#         cur = engine_conn.cursor()
#         # cur.execute("CREATE TABLE test_design (created_at, design_name, 
#         # file_location, file_name, last_updated)")

#         cur.execute("DROP TABLE IF EXISTS test_design;")
#         load_df_to_warehouse(test_df, 'test_design', engine_conn=engine_conn)
        
#         result = cur.execute("SELECT * FROM test_design")
#         results_list = result.fetchall()
#         # engine_conn.close()
#         assert results_list == [(1, '2022', 'Wooden'), (2, '2023', 'Bronze'), (3, '2023', 'Bronze')]

#     # def test_data_appends_to_existing_table_in_warehouse(self, temp_db):
#     #     test_df = pd.DataFrame(
#     #         [
#     #             {
#     #                 "design_id": 1,
#     #                 "created_at": "2022",
#     #                 "design_name": "Wooden"            
#     #             },
#     #             {
#     #                 "design_id": 2,
#     #                 "created_at": "2023",
#     #                 "design_name": "Bronze"                    
#     #             },
#     #             {
#     #                 "design_id": 3,
#     #                 "created_at": "2023",
#     #                 "design_name": "Bronze"                   
#     #             },
#     #         ]
#     #     )

#     #     test_df_2 = pd.DataFrame(
#     #         [
#     #             {
#     #                 "design_id": 4,
#     #                 "created_at": "2023",
#     #                 "design_name": "W"            
#     #             }])


#     #     # test_df.reset_index(drop=True, inplace=True)
#     #     test_df.set_index('design_id',inplace=True)
#     #     test_df_2.set_index('design_id',inplace=True)
        
#     #     engine_conn = temp_db
#     #     cur = engine_conn.cursor()
#     #     # cur.execute("CREATE TABLE test_design (created_at, design_name, 
#     #     # file_location, file_name, last_updated)")

#     #     cur.execute("DROP TABLE IF EXISTS test_design;")
#     #     load_df_to_warehouse(test_df, 'test_design', engine_conn=engine_conn)
#     #     load_df_to_warehouse(test_df_2, 'test_design', engine_conn=engine_conn)
#     #     result = cur.execute("SELECT * FROM test_design")
#     #     results_list = result.fetchall()
        
#     #     assert results_list == [(1, '2022', 'Wooden'), (2, '2023', 'Bronze'), (3, '2023', 'Bronze'), (4, '2023', 'W')]

#     # def test_appending_same_data_to_existing_table_in_warehouse_does____(self, temp_db):
#     #     test_df = pd.DataFrame(
#     #         [
#     #             {
#     #                 "design_id": 1,
#     #                 "created_at": "2022",
#     #                 "design_name": "Wooden"            
#     #             },
#     #             {
#     #                 "design_id": 2,
#     #                 "created_at": "2023",
#     #                 "design_name": "Bronze"                    
#     #             },
#     #             {
#     #                 "design_id": 3,
#     #                 "created_at": "2023",
#     #                 "design_name": "Bronze"                   
#     #             },
#     #         ]
#     #     )       


#     #     # test_df.reset_index(drop=True, inplace=True)
#     #     test_df.set_index('design_id',inplace=True)    
        
#     #     engine_conn = temp_db
#     #     cur = engine_conn.cursor()    

#     #     cur.execute("DROP TABLE IF EXISTS test_design;")
#     #     load_df_to_warehouse(test_df, 'test_design', engine_conn=engine_conn)
#     #     load_df_to_warehouse(test_df, 'test_design', engine_conn=engine_conn)

#     #     result = cur.execute("SELECT * FROM test_design")
#     #     results_list = result.fetchall()
        
#     #     assert results_list == [(1, '2022', 'Wooden'), (2, '2023', 'Bronze'), (3, '2023', 'Bronze')]


#     # def test_appending_same_data_to_existing_table_in_warehouse_does____(self, temp_db):
#     #     test_df = pd.DataFrame(
#     #         [
#     #             {                    
#     #                 "design_id": 1,                  
#     #                 "design_name": "Wooden",
#     #                 "file_location" : 'guhs',
#     #                 "file_name" : 'file'            
#     #             },
#     #             {
#     #                 "design_id": 2,                    
#     #                 "design_name": "Bronze",   
#     #                 "file_location" : 'guhs',
#     #                 "file_name" : 'file'               
#     #             },
#     #             {
#     #                 "design_id": 3,                    
#     #                 "design_name": "Bronze", 
#     #                 "file_location" : 'guhs',
#     #                 "file_name" : 'file'                  
#     #             }
#     #         ]
#     #     )       
#     #     print('p1')
#     #     load_df_to_warehouse(test_df, 'dim_design')
#     #     print('p2')
#     #     conn = connect_to_warehouse()
#     #     print('p3')
#     #     result = conn.run("""SELECT * FROM dim_design""")
#     #     print('p4')
#     #     # result_list = result.fetchall()

#     #     # test_df.reset_index(drop=True, inplace=True)
#     #     # test_df.set_index('design_id',inplace=True)    
        
#     #     # engine_conn = temp_db
#     #     # cur = engine_conn.cursor()    

#     #     # cur.execute("DROP TABLE IF EXISTS test_design;")
#     #     # cur.execute("""CREATE TABLE test_design 
#     #     #             (id INT PRIMARY KEY, design_id INT, created_at VARCHAR(4), design_name VARCHAR(10))""")  
              
#     #     # test_df.set_index('design_id',inplace=True) 
#     #     # load_df_to_warehouse(test_df, 'test_design', engine_conn=None)        
#     #     # # load_df_to_warehouse(test_df, 'test_design', engine_conn=engine_conn)

#     #     # result = cur.execute("SELECT * FROM test_design")
#     #     # results_list = result.fetchall()
#     #     pprint(result)
#     #     assert result == [(1, '2022', 'Wooden'), (2, '2023', 'Bronze'), (3, '2023', 'Bronze')]


