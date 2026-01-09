from Extraction import run_extraction
from cleaning_transformations import run_cleaning_transformations
from Modeling import run_modeling
from Visualization import run_visualization

def main():
    print("🚀 ETL Pipeline Started")

    run_extraction()
    run_cleaning_transformations()
    run_modeling()
    run_visualization()

    print("🎯 ETL Pipeline Finished Successfully")

if __name__ == "__main__":
    main()
