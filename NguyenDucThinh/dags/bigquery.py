from google.cloud import bigquery

def count_rows_in_table():
    try:
        client = bigquery.Client()
        table_id = 'gk-nguyenducthinh.demo.demo5'
        query = f"""
            SELECT COUNT(*) AS total_rows
            FROM `{table_id}`
            WHERE rating >= 3  -- Điều kiện đánh giá từ 3 sao trở lên
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            print(f"Total rows with rating >= 3 in {table_id}: {row.total_rows}")
    except Exception as e:
        print(f"An error occurred: {e}")

def most_reviews_by_user():
    try:
        client = bigquery.Client()
        table_id = 'gk-nguyenducthinh.demo.demo5'
        query = f"""
            SELECT userId, COUNT(*) AS review_count
            FROM `{table_id}`
            GROUP BY userId
            ORDER BY review_count DESC
            LIMIT 1 
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            print(f"User with most reviews: {row.userId} - Number of reviews: {row.review_count}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    count_rows_in_table()
    most_reviews_by_user()
