from flask import Flask, render_template, abort
import xmlrpc.client
import pandas as pd
import altair as alt
import pygeohash as pgh
from transport import RequestsTransport
import json
from flask import request

app = Flask(__name__)
proxy = xmlrpc.client.ServerProxy('http://0.0.0.0:2222/', transport=RequestsTransport(), allow_none=True)

month_lst = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
             'August', 'September', 'October', 'November', 'December']


@app.route("/")
def index():
    return render_template('./index.html', features=proxy.summarizer.get_feature_list())


@app.route('/correlation')
def correlation():
    return render_template('correlation.html')


@app.route('/interactive', methods=['POST', 'GET'])
def interactive():
    features = proxy.summarizer.get_feature_list()
    resolution = 'daily'
    statistic1 = 'min'
    feature1 = features[0]
    statistic2 = 'max'
    feature2 = features[0]
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
                           statistic2=statistic2, feature2=feature2, features=features)


@app.route('/generalized_chart/<feature>/<statistic>/<resolution>')
def generalized_chart_renderer(feature, statistic, resolution):
    print("generalized_chart", feature, statistic, resolution)
    data = proxy.summarizer.get_stats(feature, statistic, resolution)
    title = statistic.capitalize() + " Values for " + feature.capitalize() + " at " + resolution.capitalize() + \
        " resolution"
    if resolution == 'monthly':
        name = [i for i in range(1, 13)]
    else:
        name = [i for i in range(1, 366)]
    df_list = pd.DataFrame({'data': data, 'name': name})
    chart = (alt.Chart(data=df_list, height=300, width=450).mark_bar(tooltip={"content": "encoding"})
             .encode(
                alt.X('name', axis=alt.Axis(title=resolution), sort=None, scale=alt.Scale(domain=(1, len(name)))),
                alt.Y('data', axis=alt.Axis(title=statistic)),
                alt.Color('data', scale=alt.Scale(scheme='redblue'), sort="descending")
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
    matrix = proxy.summarizer.correlation_matrix.get_matrix()

    columns = proxy.summarizer.correlation_matrix.get_columns()
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
    chart = alt.Chart(source, height=600, width=600).mark_rect(tooltip={"content": "encoding"}).encode(
        x='x1:O',
        y='x2:O',
        color=alt.Color('correlation:Q', scale=alt.Scale(scheme='redblue'), sort="descending")
    )

    return chart.to_json()


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


@app.route('/maxStats/<feature>')
def serve_max_stats(feature):
    if feature not in proxy.summarizer.get_feature_list():
        # TODO: Make an error message
        return abort(400)
    max_stats_by_month = proxy.summarizer.get_max_stats_by_month(feature)
    df_list = pd.DataFrame({'data': max_stats_by_month, 'name': month_lst[:len(max_stats_by_month)]})
    return make_charts(df_list, 'Month', 'Maximum values', feature)


@app.route('/minStats/<feature>')
def serve_min_stats(feature):
    if feature not in proxy.summarizer.get_feature_list():
        # TODO: Make an error message
        return abort(400)
    min_stats_by_month = proxy.summarizer.get_min_stats_by_month(feature)
    df_list = pd.DataFrame({'data': min_stats_by_month, 'name': month_lst[:len(min_stats_by_month)]})
    return make_charts(df_list, 'Month', 'Minimum values', feature)


@app.route('/meanStats/<feature>')
def serve_mean_stats(feature):
    if feature not in proxy.summarizer.get_feature_list():
        # TODO: Make an error message
        return abort(400)
    mean_stats_by_month = proxy.summarizer.get_mean_stats_by_month(feature)
    df_list = pd.DataFrame({'data': mean_stats_by_month, 'name': month_lst})
    return make_charts(df_list, 'Month', 'Mean values', feature)


@app.route('/baseStats/<feature>')
def serve_base_stats(feature):
    if feature not in proxy.summarizer.get_feature_list():
        # TODO: Make an error message
        return abort(400)
    mean_stats_by_month = proxy.summarizer.get_mean_stats_by_month(feature)
    min_stats_by_month = proxy.summarizer.get_min_stats_by_month(feature)
    max_stats_by_month = proxy.summarizer.get_max_stats_by_month(feature)
    df_list = pd.DataFrame({'minimum': min_stats_by_month, 'maximum': max_stats_by_month, 'mean': mean_stats_by_month,
                            'month': month_lst})

    minmax = alt.Chart(data=df_list, height=200, width=250).mark_bar(tooltip={"content": "encoding"}).encode(
            x=alt.X('month:O', sort=None),
            y=alt.Y('minimum:Q'),
            y2=alt.Y2('maximum:Q'),
            color=alt.Color('mean:Q', scale=alt.Scale(scheme='redblue'), sort="descending")
    ).properties(
        title=feature
    )

    mean = alt.Chart(df_list).mark_tick(color='white', thickness=3, tooltip={"content": "encoding"}).encode(
        x=alt.X('month:O', sort=None),
        y='mean:Q',
        size=alt.SizeValue(20)
    )

    return (minmax + mean).to_json()


def make_charts(df, x_axis_title, y_axis_title, title):
    chart = alt.Chart(data=df, height=150, width=200).mark_bar(tooltip={"content": "encoding"})\
        .encode(
            alt.X('name', axis=alt.Axis(title=x_axis_title), sort=None),
            alt.Y('data', axis=alt.Axis(title=y_axis_title)),
            alt.Color('data', scale=alt.Scale(scheme='spectral'), sort="descending")
    ).properties(
        title=title
    )
    return chart.to_json()


if __name__ == '__main__':
    app.run()
