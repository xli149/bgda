import altair as alt
from ast import literal_eval
import pandas as pd
import preprocess
from timeit import default_timer as timer
from types import SimpleNamespace
import xmlrpc.client


def build_altair_text_error(error: str) -> str:
    return alt.Chart(pd.DataFrame({'message': [f'❌ {error}']}))\
        .mark_text(size=20, font='monospace').encode(
            text='message',
            color=alt.value('red')
        ).properties(width=500, height=50).configure_view(strokeWidth=0).to_json()


def _design_chart(title: str, query_list: list, queries: list, stat: str, stat_list: dict, parts: SimpleNamespace) -> str:
    if stat == 'base':
        y_title = 'date'
        y_val = ['.'.join(filter(None, [x.get('year', ''),
                                        x.get('month', ''), x.get('day', ''), x.get('hour', '')])) for x in queries]
        if y_val[0] == '':
            y_title = 'query'
            y_val = query_list
        df = pd.DataFrame({**{y_title: y_val}, **stat_list})
        minmax = alt.Chart(data=df, width=250).mark_bar(tooltip={"content": "encoding"}).encode(
            x=alt.X('min'),
            x2=alt.X2('max'),
            y=alt.Y(y_title, sort=None),
            color=alt.Color('mean', scale=alt.Scale(scheme='redblue'), sort="descending")
        )

        mean = alt.Chart(data=df, width=250).mark_tick(color='white', thickness=3, tooltip={"content": "encoding"})\
            .encode(
                x='mean',
                y=alt.Y(y_title, sort=None)
        )

        graph = (minmax + mean).configure_tick(
            bandSize=10  # controls the width of the tick
        ).configure_scale(
            rangeStep=10  # controls the width of the bar
        )

        if title is None:
            return graph.to_json()
        else:
            return graph.properties(title=title).to_json()

    range_ = list()
    single = list()
    none_ = list()
    aspects = {'year', 'month', 'day', 'hour', 'geohash', 'feature'}

    for part in aspects:
        if hasattr(parts, part):
            if isinstance(getattr(parts, part), list):
                range_.append(part)
            else:
                single.append(part)
        else:
            none_.append(part)

    x_title = 'query'
    x_val = query_list
    color_title = stat
    y_title = stat
    y_val = stat_list
    if len(range_) == 0 and len(single) >= 3:
        x_title = 'date'
        x_val = ['.'.join(filter(None, [x.get('year', ''),
                                        x.get('month', ''), x.get('day', ''), x.get('hour', '')])) for x in queries]
    elif len(range_) == 1:
        x_title = range_[0]
        x_val = [x[range_[0]] for x in queries]
    elif len(range_) == 2:
        x_title = range_[0]
        x_val = [x[range_[0]] for x in queries]
        y_title = range_[1]
        y_val = [y[range_[1]] for y in queries]

    df = pd.DataFrame({**{x_title: x_val}, **{y_title: y_val}, **stat_list})
    graph = alt.Chart(data=df, height=300, width=400).mark_bar(tooltip={"content": "encoding"}) \
        .encode(
        alt.X(x_title, sort=None),
        alt.Y(y_title, sort=None),
        alt.Color(color_title, scale=alt.Scale(scheme='spectral'), sort="descending")
    )

    if title is None:
        return graph.to_json()
    else:
        return graph.properties(title=title).to_json()


def build_chart(message: dict, proxy: xmlrpc.client.ServerProxy, title: str = None, default: bool = False) -> str:
    start = timer()
    query, stat = preprocess.validate_builder_message(message)
    stat_list = dict()
    parts, queries = preprocess.preprocess_query(query)
    end = timer()
    print(f'It took {end-start} to parse the query.')

    query_list = list(map(lambda x: '.'.join(x.values()), queries))
    # print(f'preprocess: {queries}')
    for q in query_list:
        try:
            stats = proxy.summarizer.execute(q)
            if stats is None:
                if default:
                    # TODO: Find a more generic way to do this
                    stats = "{'size': 0, 'max': 0.0, 'min': 0.0, 'mean': 0.0, 'variance': 0.0, 'stddev': 0.0}"
                else:
                    raise preprocess.SummarizerError(f'Unable to chart \'{q}\' from \'{query}\'')
        except xmlrpc.client.Fault:
            raise preprocess.SummarizerError('Unable to chart \'{q}\' from \'{query}\'')
        try:
            stat_info = literal_eval(stats)
        except SyntaxError:
            raise preprocess.SummarizerError('chart', build_altair_text_error(f'Unable to chart \'{q}\' from '
                                                                              f'\'{query}\''))
        # Should not have issues but it might be better to catch KeyError
        for s in stat_info.items():
            if s[0] not in stat_list:
                stat_list[s[0]] = list()
            stat_list[s[0]].append(s[1])
    # print(stat_list)

    return _design_chart(title, query_list, queries, stat, stat_list, parts)
