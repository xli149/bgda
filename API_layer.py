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
    return render_template('interactive.html', resolution=resolution, statistic1=statistic1, feature1=feature1,
                           statistic2=statistic2, feature2=feature2, features=features)


@app.route('/compare/<resolution>/<feature1>/<statistic1>/<feature2>/<statistic2>')
def compare_charts(resolution, feature1, statistic1, feature2, statistic2):
    name1 = feature1 + " " + statistic1
    name2 = feature2 + " " + statistic2
    interval = alt.selection_single(on='mouseover', nearest=True, empty='none', encodings=['x'])
    interval2 = alt.selection_interval(encodings=['x'])

    data1 = proxy.summarizer.get_stats(feature1, statistic1, resolution)
    data2 = proxy.summarizer.get_stats(feature2, statistic2, resolution)
    if resolution == 'monthly':
        time = [i for i in range(1, 13)]
    else:
        time = [i for i in range(1, 366)]
    weather_summary = pd.DataFrame({name1: data1, name2: data2, resolution: time})

    feature1_chart = alt.Chart(data=weather_summary, height=300, width=450).mark_bar(tooltip={"content": "encoding"}).encode(
        x=alt.X(resolution+':O', scale=alt.Scale(domain=interval2.ref())),
        y=alt.Y(name1+':Q'),
        color=alt.Color(name1+':Q', scale=alt.Scale(scheme='redblue'), sort="descending"),
        size=alt.condition(~interval, alt.value(3), alt.value(5))
    ).add_selection(
        interval
    )

    feature2_chart = alt.Chart(data=weather_summary, height=300, width=450).mark_bar(tooltip={"content": "encoding"}).encode(
        x=alt.X(resolution+':O', scale=alt.Scale(domain=interval2.ref())),
        y=alt.Y(name2+':Q'),
        color=alt.Color(name2+':Q', scale=alt.Scale(scheme='redblue'), sort="descending"),
        size=alt.condition(~interval, alt.value(3), alt.value(5))
    ).add_selection(
        interval
    )

    feature1_text = alt.Chart(data=weather_summary, height=300, width=450).mark_text().encode(
        x=alt.X(resolution+':O', scale=alt.Scale(domain=interval2.ref())),
        y=alt.Y(name1+':Q'),
        text=name1+':Q',
        opacity=alt.condition(interval, alt.value(1.0), alt.value(0.0)),
        color=alt.value('darkslategray')
    )

    feature2_text = alt.Chart(data=weather_summary, height=300, width=450).mark_text().encode(
        x=alt.X(resolution+':O', scale=alt.Scale(domain=interval2.ref())),
        y=alt.Y(name2+':Q'),
        text=name2+':Q',
        opacity=alt.condition(interval, alt.value(1.0), alt.value(0.0)),
        color=alt.value('darkslategray')
    )

    combined = alt.Chart(weather_summary).mark_bar(tooltip={"content": "encoding"}).encode(
        x=alt.X(resolution+':O', scale=alt.Scale(domain=interval2.ref())),
        y=alt.Y(name1+':Q'),
        y2=alt.Y2(name2+':Q'),
        color=alt.Color(name1+':Q', scale=alt.Scale(scheme='redblue'), sort="descending"),
        size=alt.condition(~interval, alt.value(3), alt.value(5))
    ).add_selection(
        interval
    )

    view = combined.properties(
        width=890,
        height=50,
        selection=interval2
    )

    # return ((feature1_chart + feature1_text) | (feature2_chart + feature2_text)).to_json()
    return (((feature1_chart + feature1_text) | (feature2_chart + feature2_text)) & view).to_json()


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
    matrix = proxy.summarizer.regressionMatrix.get_matrix()
    columns = proxy.summarizer.regressionMatrix.get_columns()
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
        x=alt.X('x1:O', axis=alt.Axis(labelAngle=-65)),
        y='x2:O',
        color=alt.Color('correlation:Q', scale=alt.Scale(scheme='redblue', domain=(-1, 1)), sort="descending")
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
            x=alt.X('minimum:Q'),
            x2=alt.X2('maximum:Q'),
            y=alt.Y('month:O', sort=None),
            color=alt.Color('mean:Q', scale=alt.Scale(scheme='redblue'), sort="descending")
    ).properties(
        title=feature
    )

    mean = alt.Chart(df_list).mark_tick(color='white', thickness=3, tooltip={"content": "encoding"}).encode(
        x='mean:Q',
        y=alt.Y('month:O', sort=None),
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
