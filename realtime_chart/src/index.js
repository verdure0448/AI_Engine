import { SciChartSurface } from "scichart/Charting/Visuals/SciChartSurface";
import { NumericAxis } from "scichart/Charting/Visuals/Axis/NumericAxis";
import { CategoryAxis } from "scichart/Charting/Visuals/Axis/CategoryAxis";
import { XyDataSeries } from "scichart/Charting/Model/XyDataSeries";
import { FastLineRenderableSeries } from "scichart/Charting/Visuals/RenderableSeries/FastLineRenderableSeries";
import { XyScatterRenderableSeries } from "scichart/Charting/Visuals/RenderableSeries/XyScatterRenderableSeries";
import { EllipsePointMarker } from "scichart/Charting/Visuals/PointMarkers/EllipsePointMarker";
import { NumberRange } from "scichart/Core/NumberRange";
import { RubberBandXyZoomModifier } from "scichart/Charting/ChartModifiers/RubberBandXyZoomModifier";
import { ZoomExtentsModifier } from "scichart/Charting/ChartModifiers/ZoomExtentsModifier";
import { ZoomPanModifier } from "scichart/Charting/ChartModifiers/ZoomPanModifier";
import { EZoomState } from "scichart/types/ZoomState";
import { NumericLabelProvider } from "scichart/Charting/Visuals/Axis/LabelProvider/NumericLabelProvider";
import { ENumericFormat } from "scichart/types/NumericFormat";
import { EAutoRange } from "scichart/types/AutoRange";

const { InfluxDB } = require('@influxdata/influxdb-client');
const influx = new InfluxDB({
    url: "http://9.8.100.156:8086",
    token: "uOpIW55Map8EuwijejVYQkSlwtq1J_C8etbJxrRyOdl7jjS8cVRRKLnjJHmDSKs-urArRwqZYKlJqa3cxNZsNg=="
})

const influxdb = influx
const influxQuery = influxdb.getQueryApi('HN')

let trendList = [];
let predictList = [];
let initTrendList = [];
let initPredictList = [];
let startTime = 0;
let queryState = false

function convertTime(rfcTime) {
    let unixTime = new Date(Date.parse(rfcTime) - (900 * 60 * 1000))

    return unixTime.getTime()/1000
}

function initData() {
    const initQuery = 'from(bucket: "MH001001001-CNC001-detection") |> range(start: -180s, stop: now()) |> filter(fn: (r) => r["_measurement"] == "OP10-3") |> filter(fn: (r) => r["_field"] == "Trend" or r["_field"] == "PredictData") |> aggregateWindow(every: 100ms, fn: mean, createEmpty: false)';
    influxQuery.queryRows(
        initQuery, {
            next(row, tableMeta) {
                const o = tableMeta.toObject(row);
                if (o._field == "Trend") {
                    initTrendList.push(o);
                } else if (o._field == "PredictData") {
                    initPredictList.push(o)
                }
            }, error(error) {
                console.log("[ERROR] : in init_data function")
                console.log(error)
            }, complete() {
                console.log("[INFO] : complete in init_data function ", initTrendList.length)
                if (initTrendList.length != 0) {
                    const lastTime = initTrendList[initTrendList.length - 1]._time
                    startTime = new Date(Date.parse(lastTime) + 100).toISOString()
                    queryState = true
                }
            }
        }
    )
}

async function initSciChart() {
    SciChartSurface.setRuntimeLicenseKey("rBx8hbMPgUvxi7m2lmS35nKb2dNvWxy366vZ7KN3zQlCbNnDSgHprUE4aXL9ots2g+aa0Qw23NA1ogKuZKTFjAnvD9waio/VbW6jsxbwfYdJ8ysLECnRAq4qa7o5Gbre205FLB9WvzF/mqj1NWz97gH8gd0s/8j8pqjr0bHfWZXq4ZIwCRr6xU4CfPnDOFXvztaJ7D+VlAqrCFGSCb6isOXJ+STgg3vb00xkXF+HjHhsR24SqsWW1RDbXWOfljeOXgOA+5Xh0bbunhRakRFV5deR8yRopNyPGLFsTID6Sa2QlAhcNVWXR7R1hEbT1aaCsPkg/zgwY2kIcImXse952OQFUoNTaYezIXA2Bvx7xg6cEu0DKoG6KgKnmD7HnaPxM6EUv4ZO3nBPkh5yKpWl7SEHYJzisoVyHLHWI+XiM4FnIQy5XseO6FYD2nsvhVFLbJkhBpom/tfB3u3jUGAx5ZXoqfqDXynuAvuJnMjM3ww96XfoDv/KoNwXrl9lGtCl5V7o+d9yY8NOb2D3MUFqsuOVnC0fPjxiqabCzYNvuZabYg==");
    const { sciChartSurface, wasmContext } = await SciChartSurface.create("scichart-root");

    const xAxis = new CategoryAxis(wasmContext);
    xAxis.labelProvider.numericFormat = ENumericFormat.Date_HHMM;
    xAxis.visibleRangeLimit = new NumberRange(1,10000);
    xAxis.growBy = new NumberRange(0.0, 0.1);
    xAxis.autoRange = EAutoRange.Never;

    const yAxis = new NumericAxis(wasmContext);
    yAxis.visibleRange = new NumberRange(0,1)

    sciChartSurface.xAxes.add(xAxis);
    sciChartSurface.yAxes.add(yAxis);

    // Create a Scatter series, and Line series and add to chart
    const rowDataSeries = new FastLineRenderableSeries(wasmContext, { stroke: "#4083B7", strokeThickness: 2 });
    const predictDataSeries = new FastLineRenderableSeries(wasmContext, { stroke: "#b77a40", strokeThickness: 2 });
    sciChartSurface.renderableSeries.add(rowDataSeries, predictDataSeries);

    // Create and populate some XyDataSeries with static data
    // Note: you can pass xValues, yValues arrays to constructors, and you can use appendRange for bigger datasets
    const rowDatas = new XyDataSeries(wasmContext, { dataSeriesName: "row" });
    const predictDatas = new XyDataSeries(wasmContext, { dataSeriesName: "predict" });
    
    const trendQueue = [];
    const predictQueue = [];

    const getQuery = () => {
        trendList = [];
        predictList = [];
        let query = 'from(bucket: "MH001001001-CNC001-detection") |> range(start: time(v:' + (startTime) + '), stop: now()) |> filter(fn: (r) => r["_measurement"] == "OP10-3") |> filter(fn: (r) => r["_field"] == "Trend" or r["_field"] == "PredictData") |> aggregateWindow(every: 100ms, fn: mean, createEmpty: false)';
        influxQuery.queryRows(
            query,
            {
                next(row, tableMeta) {
                    const o = tableMeta.toObject(row);
                    if(o._field == "Trend") {
                        trendList.push(o);
                    } else if(o._field == "PredictData") {
                        predictList.push(o)
                    }
                },
                error(error) {
                    console.log("[ERROR] : in getQuery function")
                    console.error(error);
                },
                complete() {
                    console.log("[IFNO] : complete : ", trendList.length)
                    if(trendList.length != 0) {
                        const lastTime = trendList[trendList.length-1]._time
                        startTime = new Date(Date.parse(lastTime) + 100).toISOString()
                        queryState = true
                    }
                    
                },
            }
        );
        setTimeout(getQuery ,8000);
    }

    let initProcess = true
    for(let i=0; i<1000; i++) {
        const initTrendData = initTrendList.shift()
        const initPredictData = initPredictList.shift()
        rowDatas.append(convertTime(initTrendData._time), initTrendData._value)
        predictDatas.append(convertTime(initPredictData._time), initPredictData._value)
    }

    rowDataSeries.dataSeries = rowDatas;
    predictDataSeries.dataSeries = predictDatas;

    // Add ZoomExtentsModifier and disable extends animation
    sciChartSurface.chartModifiers.add(new ZoomExtentsModifier({ isAnimated: false }));
    // Add RubberBandZoomModifier
    sciChartSurface.chartModifiers.add(new RubberBandXyZoomModifier());
    // Add ZoomPanModifier
    // sciChartSurface.chartModifiers.add(new ZoomPanModifier());

    const updateDataFunc = () => {
        if(initTrendList.length != 0 && initProcess == true) {
            const initTrendData = initTrendList.shift()
            const initPredictData = initPredictList.shift()
            rowDatas.append(convertTime(initTrendData._time), initTrendData._value)
            predictDatas.append(convertTime(initPredictData._time), initPredictData._value)
        } else {
            initProcess = false
        }
        if(queryState != false) {
            trendQueue.push.apply(trendQueue, trendList)
            predictQueue.push.apply(predictQueue, predictList)
            queryState = false
        }

        // Append another data-point to the chart. We use dataSeries.count()
        // to determine the current length before appending
        const i = rowDatas.count();
        
        if(trendQueue.length != 0 && initProcess == false) {
            const trendData = trendQueue.shift()
            const predictData = predictQueue.shift()
            //console.log(convertTime(trendData._time))
            rowDatas.append(convertTime(trendData._time), trendData._value)
            predictDatas.append(convertTime(predictData._time), predictData._value) 
        }
        //lineData.append(new Date().getTime()/1000, 1)
        //scatterData.append(new Date().getTime()/1000, 0.6)
        
        // ZoomExtents after appending data.
        // Also see XAxis.AutoRange, and XAxis.VisibleRange for more options
        if (sciChartSurface.zoomState !== EZoomState.UserZooming) {
            xAxis.visibleRange = new NumberRange(i - 1000, i);
        }

        setTimeout(updateDataFunc, 100)
    };
    getQuery();
    updateDataFunc();
}
initData();
initSciChart();