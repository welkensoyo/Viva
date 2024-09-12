import streamlit as st
import altair as alt
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from nfty.sflake import API as sflake_API, report_dict, d_cols, create_month_year_index, facility_names


@st.cache_data(ttl=60 * 60 * 4)
def load_report(report='patients_seen', where=None):
    s = sflake_API(where=where)
    if report == 'charts':
        x = s.charts()
        return pd.json_normalize(x[0]), pd.json_normalize(x[1]), pd.json_normalize(x[2])
    return pd.json_normalize(s.report(report))

custom_agg_distinct_js = JsCode("""
function customDistinctCount(params) {
    const uniqueValues = new Set(params.values);
    return uniqueValues.size;
}
""")
custom_agg_sum_js = JsCode("""
function customSum(params) {
    const sum = params.values.reduce((total, value) => total + value, 0);
    return Math.round(sum * 100) / 100;
}
""")

def app():
    params = st.query_params
    st.set_page_config(layout="wide")
    # st.subheader('Viva')
    st.sidebar.title("Viva Metrics")
    st.markdown("""
        <style>
            #MainMenu {visibility: show;}
            .stDeployButton {display:none;}
            footer {visibility: hidden;}
            #stDecoration {display:none;}
        </style>
    """, unsafe_allow_html=True)

    # if st.button("Show Grid State"):
    #     st.write(pprint.pformat(st.session_state.get("_grid_state", {})))

    # Convert the list into a pandas DataFrame
    report_select = st.sidebar.selectbox(
        "Select Report",
        tuple(report_dict.keys()),
    )

    if 'chart' not in report_select.lower():
        if report_select in('Primary and Secondary Payor',):
            col1, col2 = st.columns([2, 2])
            with col1:
                selected_facility = st.selectbox("Select a Facility", facility_names)
            df = load_report(report_dict[report_select].get('name'), where=selected_facility)
        else:
            df = load_report(report_dict[report_select].get('name'))
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, editable=True, enableRangeSelection=True, filterable=True)
        for d in df.columns:
            if d in d_cols:
                if d_cols[d] == 'SET':
                    gb.configure_column(field=d, filter='agSetColumnFilter')
                if d_cols[d] == 'DATE':
                    df[d] = df[d].apply(lambda x: x.strftime('%Y-%m-%d') if not pd.isnull(x) else '')
                    gb.configure_column(field=d, type='dateColumn', filter=True)
                if d_cols[d] == 'NOFILTER':
                    gb.configure_column(field=d, filter=False)
            elif df[d].dtype in ('int64', 'float64'):
                if d == 'USERID':
                    gb.configure_column("USERID", aggFunc='distinct')
                else:
                    gb.configure_column(field=d, type='numericColumn', precision=2, filter='agNumberColumnFilter', aggFunc='sum2d')
            else:
                gb.configure_column(field=d, filter='agMultiColumnFilter')
            if d =='WEEK_END':
                gb.configure_column("YEAR", rowGroup=True, enableRowGroup=True)
                gb.configure_column("WEEK_END", rowGroup=True, enableRowGroup=True)
        if 'YEAR' in df.columns:
            df.sort_values(by=['YEAR', 'MONTH'], ascending=[False, False], inplace=True)
        resize = params.get('resize') or report_dict[report_select].get('resize')
        # In Case you want to autosize the columns instead
        if resize:
            column_widths = {col: df[col].astype(str).map(len).max() for col in df.columns}
            gb.configure_columns([{'headerName': col, 'field': col, 'width': max(50, column_widths[col] * 6), 'editable': True} for col in df.columns])
        # for col in cdefs.get(report_select, []):
        #     if col:
        #         gb.configure_column(col)

        sidebar = {
            'toolPanels': ['filters', 'columns'],
            'defaultToolPanel': ''
        }
        gb.configure_side_bar(filters_panel=True, columns_panel=True, defaultToolPanel=sidebar['toolPanels'])
        gb.configure_side_bar(sidebar)
        grid_options = gb.build()
        grid_options['aggFuncs'] = {
            'distinct': custom_agg_distinct_js,
            'sum2d': custom_agg_sum_js
        }

        # Call `load_report()` with the selected report name
        st.write(report_select)
        AgGrid(df,
               gridOptions=grid_options,
               height=600,
               width='100%',
               data_return_mode='as_input',
               update_mode='value_changed',
               fit_columns_on_grid_load=False if resize else True,
               allow_unsafe_jscode=True,  # Set it to True to enable jsfunction
               enable_enterprise_modules=True,  # Set it to True to enable enterprise modules
               license_key='Using_this_AG_Grid_Enterprise_key_( AG-043994 )_in_excess_of_the_licence_granted_is_not_permitted___Please_report_misuse_to_( legal@ag-grid.com )___For_help_with_changing_this_key_please_contact_( info@ag-grid.com )___( Triple Play Pay )_is_granted_a_( Single Application )_Developer_License_for_the_application_( Triple Play Pay )_only_for_( 1 )_Front-End_JavaScript_developer___All_Front-End_JavaScript_developers_working_on_( Triple Play Pay )_need_to_be_licensed___( Triple Play Pay )_has_been_granted_a_Deployment_License_Add-on_for_( 1 )_Production_Environment___This_key_works_with_AG_Grid_Enterprise_versions_released_before_( 21 June 2024 )____[v2]_MTcxODkyNDQwMDAwMA==2715c856a3cb3ab5c966698c55c41fac'
               # This should be your actual ag-grid license key
               )
    else:
        nurses, nonurses, acuity = load_report('charts')
        month_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                     7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
        mvals = list(month_map.values())
        month_mmm_map = create_month_year_index()
        # nurses = nurses[nurses['DISCIPLINE'].isin(['RN', 'LVN'])].groupby(nurses.columns.drop('DISCIPLINE').tolist()).size().reset_index().rename(columns={0: 'count'})
        billed = nurses.melt(id_vars='MONTH', value_vars=['HOURS_THIS_YEAR', 'HOURS_LAST_YEAR'])
        ot = nurses.melt(id_vars='MONTH', value_vars=['OT_%_THIS_YEAR', 'OT_%_LAST_YEAR'])
        unbilled = nurses.melt(id_vars='MONTH', value_vars=['UN_BILLED_THIS_YEAR'])
        acuity_compared = acuity.melt(id_vars='MONTH_ABBR', value_vars=['HIGH_ACUITY_%', 'PRIOR_YEAR_%'])
        billed['MONTH'] = billed['MONTH'].map(month_map)
        ot['MONTH'] = ot['MONTH'].map(month_map)
        chart = alt.Chart(billed).encode(x=alt.X('MONTH:O', sort=mvals, title=None), y=alt.Y('value', title='Nurse Billable Hours'), color='variable').mark_line()
        chart += alt.Chart(billed).mark_point().encode(x=alt.X('MONTH:O', sort=mvals, title=None), y=alt.Y('value', title='Nurse Billable Hours'), color='variable')
        chart2 = alt.Chart(ot).encode(x=alt.X('MONTH:O', sort=mvals, title=None), y=alt.Y('value', title='Nurse OT %'), color='variable').mark_line()
        chart2 += alt.Chart(ot).mark_point().encode(x=alt.X('MONTH:O', sort=mvals, title=None), y=alt.Y('value', title='Nurse OT %'), color='variable')
        acuity_chart = alt.Chart(acuity_compared).encode(x=alt.X('MONTH_ABBR:O', sort=mvals, title=None), y=alt.Y('value', title='Acuity %'), color='variable').mark_line()
        acuity_chart += alt.Chart(acuity_compared).mark_point().encode(x=alt.X('MONTH_ABBR:O', sort=mvals, title=None), y=alt.Y('value', title='Acuity %'), color='variable')
        current_month = pd.to_datetime('today').month
        last_month = current_month - 1 if current_month != 1 else 12
        unbilled = unbilled[unbilled['MONTH'] <= last_month-1]
        unbilled['MONTH'] = unbilled['MONTH'].map(month_map)
        bar = alt.Chart(unbilled).mark_bar(width=10).encode(
            x=alt.X('MONTH:O', sort=list(month_map.values()), title=None, scale=alt.Scale(nice=True)),
            y=alt.Y('value', title='Unbilled Hours'))
        bar += alt.Chart(unbilled).mark_rule(color='red').encode(y=alt.Y('mean(value):Q', title='Unbilled Hours'))
        st.altair_chart(chart | chart2 | bar, use_container_width=True)
        st.altair_chart(acuity_chart, use_container_width=True)

if __name__ == '__main__':
    app()
