from flask import Flask, render_template
import xmlrpc.client
import pandas as pd
from altair import Chart, X, Y, Axis, Scale
import pygeohash as pgh
from transport import RequestsTransport
import json
from flask import request

app = Flask(__name__)
proxy = xmlrpc.client.ServerProxy('http://0.0.0.0:2222/', transport=RequestsTransport(), allow_none=True)

month_lst = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
             'August', 'September', 'October', 'November', 'December']

height = 150
width = 200


@app.route("/")
def index():
    return render_template('./index.html')


@app.route('/correlation')
def correlation():
    return render_template('correlation.html')


@app.route('/interactive', methods=['POST', 'GET'])
def interactive():

    resolution = 'daily'
    statistic1 = 'min'
    feature1 = 'AIR_TEMPERATURE'
    statistic2 = 'max'
    feature2 = 'AIR_TEMPERATURE'
    if request.method == 'POST':
        resolution = request.form['resolution']
        statistic1 = request.form['statistic1']
        feature1 = request.form['feature1']
        statistic2 = request.form['statistic2']
        feature2 = request.form['feature2']
        print(resolution)
        print(statistic1)
        print(feature1)
        print(statistic2)
        print(feature2)
    return render_template('interactive.html', resolution=resolution, statistic1=statistic1, feature1=feature1,
                           statistic2=statistic2, feature2=feature2)


@app.route('/generalized_chart/<feature>/<statistic>/<resolution>')
def generalized_chart_renderer(feature, statistic, resolution):
    print("generalized_chart", feature, statistic, resolution)
    # FIXME: Either use global definition, remove global definition, or rename width and height
    width = 400
    height = 250
    data = proxy.summarizer.get_stats(feature, statistic, resolution)
    title = statistic.capitalize() + " Values for " + feature.capitalize() + " at " + resolution.capitalize() + \
        " resolution"
    if resolution == 'monthly':
        name = [i for i in range(1, 13)]
    else:
        name = [i for i in range(1, 366)]
    df_list = pd.DataFrame({'data': data, 'name': name})
    chart = (Chart(data=df_list, height=height, width=width).mark_bar(color="red", tooltip={"content": "encoding"})
             .encode(
                X('name', axis=Axis(title=resolution), sort=None, scale=Scale(domain=(1, len(name)))),
                Y('data', axis=Axis(title=statistic))
    )).properties(
        title=title
    )
    return chart.to_json()


@app.route('/corr/<this>/<that>')
def serve_corr(this, that):
    return str(proxy.summarizer.regressionMatrix.correlation(this, that))

@app.route('/slope/<this>/<that>')
def serve_slope(this, that):
    return str(proxy.summarizer.regressionMatrix.slope(this, that))

@app.route('/intercept/<this>/<that>')
def serve_intercept(this, that):
    return str(proxy.summarizer.regressionMatrix.intercept(this, that))


@app.route('/corr_matrix')
def correlation_matrix():
    matrix = proxy.summarizer.regressionMatrix.get_matrix()
    columns = ['UTC_DATE',
               'UTC_TIME',
               'LONGITUDE',
               'LATITUDE',
               'AIR_TEMPERATURE',
               'PRECIPITATION',
               'SOLAR_RADIATION',
               'SURFACE_TEMPERATURE',
               'RELATIVE_HUMIDITY']
    x1 = []
    x2 = []
    correlations = []
    for i, row in enumerate(matrix):
        for j, item in enumerate(row):
            x1.append(columns[i])
            x2.append(columns[j])
            correlations.append(matrix[i][j])

    source = pd.DataFrame({'x1': x1,
                           'x2': x2,
                           'correlation': correlations})
    chart = Chart(source, height=600, width=600).mark_rect(tooltip={"content": "encoding"}).encode(
        x='x1:O',
        y='x2:O',
        color='correlation:Q'
    )

    return chart.to_json()

@app.route('/query/<query>')
def execute_query(query):
    stats = proxy.summarizer.execute(query)
    if stats is None:
        return "no data"

    return stats


@app.route('/max/<day>/<feature>')
def serve_max_for_day(day, feature):
    day = int(day)
    return str(proxy.summarizer.get_max_for_day(day, feature))


@app.route('/min/<day>/<feature>')
def serve_min_for_day(day, feature):
    day = int(day)
    return str(proxy.summarizer.get_min_for_day(day, feature))


@app.route('/mean/<day>/<feature>')
def serve_mean_for_day(day, feature):
    day = int(day)
    return str(proxy.summarizer.get_mean_for_day(day, feature))


@app.route('/variance/<day>/<feature>')
def serve_variance_for_day(day, feature):
    day = int(day)
    return str(proxy.summarizer.get_variance_for_day(day, feature))


@app.route('/showLocations')
def serve_unique_location():
    locations = proxy.summarizer.get_unique_location()
    return str(locations)


@app.route('/maxStats/AIR_TEMPERATURE')
def serve_max_stats_air_temp():
    max_stats_by_month = proxy.summarizer.get_max_stats_by_month('AIR_TEMPERATURE')
    df_list = pd.DataFrame({'data': max_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#ff0000', 'Month', 'Maximum values', 'AIR_TEMPERATURE')


@app.route('/maxStats/PRECIPITATION')
def serve_max_stats_prep():
    max_stats_by_month = proxy.summarizer.get_max_stats_by_month('PRECIPITATION')
    df_list = pd.DataFrame({'data': max_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#0039e6', 'Month', 'Maximum values', 'PRECIPITATION')


@app.route('/maxStats/SOLAR_RADIATION')
def serve_max_stats_solar_radiation():
    max_stats_by_month = proxy.summarizer.get_max_stats_by_month('SOLAR_RADIATION')
    df_list = pd.DataFrame({'data': max_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#26734d', 'Month', 'Maximum values', 'SOLAR_RADIATION')


@app.route('/maxStats/SURFACE_TEMPERATURE')
def serve_max_stats_surface_temp():
    max_stats_by_month = proxy.summarizer.get_max_stats_by_month('SURFACE_TEMPERATURE')
    df_list = pd.DataFrame({'data': max_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#993d00', 'Month', 'Maximum values', 'SURFACE_TEMPERATURE')


@app.route('/maxStats/RELATIVE_HUMIDITY')
def serve_max_stats_relative_humidity():
    max_stats_by_month = proxy.summarizer.get_max_stats_by_month('RELATIVE_HUMIDITY')
    df_list = pd.DataFrame({'data': max_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#8000ff', 'Month', 'Maximum values', 'RELATIVE_HUMIDITY')


@app.route('/minStats/AIR_TEMPERATURE')
def serve_min_stats_air_temp():
    min_stats_by_month = proxy.summarizer.get_min_stats_by_month('AIR_TEMPERATURE')
    min_stats_by_month = [-1 if a == 10000 else a for a in min_stats_by_month]
    df_list = pd.DataFrame({'data': min_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#ffcccc', 'Month', 'Minimum values', 'AIR_TEMPERATURE')


@app.route('/minStats/PRECIPITATION')
def serve_min_stats_prep():
    min_stats_by_month = proxy.summarizer.get_min_stats_by_month('PRECIPITATION')
    min_stats_by_month = [-1 if a == 10000 else a for a in min_stats_by_month]
    df_list = pd.DataFrame({'data': min_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#b3c6ff', 'Month', 'Minimum values', 'PRECIPITATION')


@app.route('/minStats/SOLAR_RADIATION')
def serve_min_stats_solar_radiation():
    min_stats_by_month = proxy.summarizer.get_min_stats_by_month('SOLAR_RADIATION')
    min_stats_by_month = [-1 if a == 10000 else a for a in min_stats_by_month]
    df_list = pd.DataFrame({'data': min_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#b4e4cd', 'Month', 'Minimum values', 'SOLAR_RADIATION')


@app.route('/minStats/SURFACE_TEMPERATURE')
def serve_min_stats_surface_temp():
    min_stats_by_month = proxy.summarizer.get_min_stats_by_month('SURFACE_TEMPERATURE')
    min_stats_by_month = [-1 if a == 10000 else a for a in min_stats_by_month]
    df_list = pd.DataFrame({'data': min_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#ffc299', 'Month', 'Minimum values', 'SURFACE_TEMPERATURE')


@app.route('/minStats/RELATIVE_HUMIDITY')
def serve_min_stats_relative_humidity():
    min_stats_by_month = proxy.summarizer.get_min_stats_by_month('RELATIVE_HUMIDITY')
    min_stats_by_month = [-1 if a == 10000 else a for a in min_stats_by_month]
    df_list = pd.DataFrame({'data': min_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#d9b3ff', 'Month', 'Minimum values', 'RELATIVE_HUMIDITY')


@app.route('/meanStats/AIR_TEMPERATURE')
def serve_mean_stats_air_temp():
    mean_stats_by_month = proxy.summarizer.get_mean_stats_by_month('AIR_TEMPERATURE')
    df_list = pd.DataFrame({'data': mean_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#ff8080', 'Month', 'Mean values', 'AIR_TEMPERATURE')


@app.route('/meanStats/PRECIPITATION')
def serve_mean_stats_prep():
    mean_stats_by_month = proxy.summarizer.get_mean_stats_by_month('PRECIPITATION')
    df_list = pd.DataFrame({'data': mean_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#668cff', 'Month', 'Mean values', 'PRECIPITATION')


@app.route('/meanStats/SOLAR_RADIATION')
def serve_mean_stats_solar_radiation():
    mean_stats_by_month = proxy.summarizer.get_mean_stats_by_month('SOLAR_RADIATION')
    df_list = pd.DataFrame({'data': mean_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#44bb81', 'Month', 'Mean values', 'SOLAR_RADIATION')


@app.route('/meanStats/SURFACE_TEMPERATURE')
def serve_mean_stats_surface_temp():
    mean_stats_by_month = proxy.summarizer.get_mean_stats_by_month('SURFACE_TEMPERATURE')
    df_list = pd.DataFrame({'data': mean_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#ff944d', 'Month', 'Mean values', 'SURFACE_TEMPERATURE')


@app.route('/meanStats/RELATIVE_HUMIDITY')
def serve_mean_stats_relative_humidity():
    mean_stats_by_month = proxy.summarizer.get_mean_stats_by_month('RELATIVE_HUMIDITY')
    df_list = pd.DataFrame({'data': mean_stats_by_month, 'name': month_lst})
    return make_charts(df_list, '#bf80ff', 'Month', 'Mean values', 'RELATIVE_HUMIDITY')

def make_charts(df, color, x_axis_title, y_axis_title, title):
    chart = Chart(data=df, height=height, width=width).mark_bar(color=color, tooltip={"content": "encoding"}).encode(
        X('name', axis=Axis(title=x_axis_title), sort=None),
        Y('data', axis=Axis(title=y_axis_title))
    ).properties(
        title=title
    )
    return chart.to_json()


if __name__ == '__main__':
    app.run()
