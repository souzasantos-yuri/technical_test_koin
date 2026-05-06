def main():
    print("Hello from case-koin!")


if __name__ == "__main__":
    main()








def save_data(df: pd.DataFrame, output_path: str) -> pd.DataFrame:
   return df.to_csv(output_path, index=False, sep=",", encoding="utf-8")