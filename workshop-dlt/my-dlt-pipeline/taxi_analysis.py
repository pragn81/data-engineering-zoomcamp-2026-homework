import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        import marimo as mo
        import dlt
        import duckdb

        con = duckdb.connect(database="taxi_pipeline.duckdb", read_only=True)
        df_taxis = con.execute("SELECT * FROM nyc_taxi.nyc_taxi_data LIMIT 100").df()

        # Si queremos usan polars en vez de pandas
        # df_taxis = con.execute("SELECT * FROM nyc_taxi.nyc_taxi_data LIMIT 100").pl()

        # Mostrar en Marimo
        mo.ui.table(df_taxis)
        """
    )
    return


if __name__ == "__main__":
    app.run()
