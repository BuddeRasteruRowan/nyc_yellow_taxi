use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::{count, sum, avg};
use datafusion::functions::datetime::expr_fn::date_part;


#[tokio::main]
async fn main() -> Result<()> {

    let ctx = SessionContext::new();

    let parquet_path = "parquet_files/"; 
    ctx.register_parquet("trips", parquet_path, ParquetReadOptions::default()).await?;

    //Aggregation 1

    let month_api = ctx.table("trips").await?
        .with_column("pickup_month", date_part(lit("month"), col("tpep_pickup_datetime")))?
        .aggregate(
            vec![col("pickup_month")],
            vec![
                count(lit(1)).alias("trip_count"),
                sum(col("total_amount")).alias("total_revenue"),
                avg(col("fare_amount")).alias("avg_fare"),
            ],
        )?
        .sort(vec![col("pickup_month").sort(true, false)])?;

    println!("----- Trips and Revenue by Month Dataframe API -----");
    month_api.show().await?;

    let month_sql = "
        SELECT 
            DATE_PART('month', tpep_pickup_datetime) AS pickup_month,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue,
            AVG(fare_amount) AS avg_fare
        FROM trips
        GROUP BY 1
        ORDER BY 1 ASC
    ";
    println!("----- Trips and Revenue by Month SQL -----");
    ctx.sql(month_sql).await?.show().await?;


// Aggregation 2:

let payment_api = ctx.table("trips").await?
    .aggregate(
        vec![col("payment_type")],
        vec![
            count(lit(1)).alias("trip_count"),
            sum(col("tip_amount")).alias("total_tips"),
            sum(col("total_amount")).alias("total_revenue"),
            avg(col("tip_amount")).alias("avg_tip_amount"),
        ],
    )?
    .with_column("tip_rate", col("total_tips") / col("total_revenue"))?

    .select(vec![
        col("payment_type"),
        col("trip_count"),
        col("avg_tip_amount"),
        col("tip_rate"),
    ])?
    .sort(vec![col("trip_count").sort(false, false)])?;

    println!("----- Tip Behavior by Payment Type Dataframe API -----");
    payment_api.show().await?;

    let tips_sql = "
        SELECT 
            payment_type,
            COUNT(*) AS trip_count,
            AVG(tip_amount) AS avg_tip_amount,
            SUM(tip_amount) / SUM(total_amount) AS tip_rate
        FROM trips
        GROUP BY payment_type
        ORDER BY trip_count DESC
    ";
    println!("----- Tip Behavior by Payment Type SQL -----");
    ctx.sql(tips_sql).await?.show().await?;

    Ok(())
}