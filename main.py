import sys
import traceback

print("=" * 80)
print("Starting main.py execution")
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
print(f"Current working directory: {sys.path}")
print("=" * 80)

try:
    print("\n[1/4] Importing cc_raw_data_pull...")
    from src.ingestion.cc_raw_data_pull import download_data_from_each_index
    print("✓ Successfully imported download_data_from_each_index")
    
    print("\n[2/4] Importing cc_metadata_pull...")
    from src.ingestion.cc_metadata_pull import push_metadata_to_postgres
    print("✓ Successfully imported push_metadata_to_postgres")
    
    print("\n[3/4] Running push_metadata_to_postgres()...")
    push_metadata_to_postgres()
    print("✓ push_metadata_to_postgres() completed")
    
    print("\n[4/4] Running download_data_from_each_index()...")
    download_data_from_each_index()
    print("✓ download_data_from_each_index() completed")
    
    print("\n" + "=" * 80)
    print("✓ main.py execution completed successfully!")
    print("=" * 80)
    
except Exception as e:
    print("\n" + "=" * 80)
    print(f"✗ ERROR: {type(e).__name__}")
    print(f"Message: {str(e)}")
    print("=" * 80)
    traceback.print_exc()
    sys.exit(1)