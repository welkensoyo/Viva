import uuid
import streamlit as st
import altair as alt
import pandas as pd
import json
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, GridUpdateMode, DataReturnMode
from nfty.sflake import API as sflake_API, report_dict, d_cols, create_month_year_index, facility_names
import extra_streamlit_components as stx
import streamlit_option_menu as sm
import datetime
from nfty.aggrid_utils import configure_grid_state, custom_agg_distinct_js, custom_agg_sum_js, custom_css, decimal2
from nfty.cache import user_cache
from tools.process_upload import ProcessFile

@st.cache_data(ttl=60 * 60 * 12)
def load_report(report='patients_seen', where=None):
    s = sflake_API(where=where)
    if report == 'charts':
        x = s.charts()
        return pd.json_normalize(x[0]), pd.json_normalize(x[1]), pd.json_normalize(x[2])
    if report == 'rollup':
        df = pd.json_normalize(s.report(report))
        columns = df.columns.tolist()
        if not columns:
            return pd.DataFrame()
        columns.remove('WEEK_END')
        columns_sorted = sorted(columns)
        columns_sorted = ['WEEK_END'] + columns_sorted
        return df[columns_sorted]
    return pd.json_normalize(s.report(report))

def load_uploaded_file(uploaded_file):
    if uploaded_file.type == "text/csv":
        return pd.read_csv(uploaded_file)
    elif uploaded_file.type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        return pd.read_excel(uploaded_file)
    return None

def save_state(filepath, state):
    with open(filepath, "w") as f:
        json.dump(state, f)

def u_file():
    uploaded_file = st.file_uploader("Upload a file", type=["csv", "xlsx"])
    if uploaded_file is not None:
        # file_details = {
        #     "filename": uploaded_file.name,
        #     "filetype": uploaded_file.type,
        #     "filesize": uploaded_file.size
        # }
        df = load_uploaded_file(uploaded_file)
        st.dataframe(df)
        if st.button('Process File'):
            with st.spinner(f'{uploaded_file.name} Processing...'):
                ProcessFile(uploaded_file).process_upload()
            st.write(f'{uploaded_file.name} Processed Successfully.')

def display_report(report_select, id):
    # Read saved grid state from file if it exists
    saved_state = {}
    if x:= user_cache(id):
        saved_state = x[0]

    if report_select in ('Primary and Secondary Payor',):
        col1, col2 = st.columns([2, 2])
        with col1:
            selected_facility = st.selectbox("Select a Facility", facility_names)
        df = load_report(report_dict[report_select].get('name'), where=selected_facility)
    elif report_select == 'Uploaded Report':
        return
    else:
        df = load_report(report_dict[report_select].get('name'))
    if state := saved_state.get(report_select, {}) or []:
        try:
            new_column_order = [item['field'] for item in state if isinstance(item, dict)]
            valid_column_order = [col for col in new_column_order if col in df.columns]
            df = df[valid_column_order]
        except:
            pass
    gb = GridOptionsBuilder.from_dataframe(df)
    # gb.configure_grid_options(alwaysShowHorizontalScroll=True, enableRangeSelection=True, pagination=True, paginationPageSize=10000, domLayout='normal')
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, enablePivot=True, editable=True, enableRangeSelection=True, filterable=True)
    expanded_groups = []
    apply_filter_js = ''
    if state := saved_state.get(report_select, {}) or []:
        expanded_groups = state.pop()
        for c in state:
            if df[c.get('field')].dtype in ('int64', 'float64'):
                gb.configure_column(headerName=c.get('headerName', c.get('field')), field=c['field'], type=c['type'], filter=c.get('filter', ''), aggFunc=c.get('aggFunc', ''), sort=c.get('sort'),
                                    enableRowGroup=c.get('enableRowGroup', False),rowGroup=c.get('rowGroup', False), order=c.get('order', ''), hide=c.get('hide', False), width=c.get('width', ''), valueFormatter=decimal2)

            else:
                gb.configure_column(headerName=c.get('headerName', c.get('field')), field=c['field'], type=c['type'], filter=c.get('filter', ''), aggFunc=c.get('aggFunc', ''), sort=c.get('sort'),
                                    enableRowGroup=c.get('enableRowGroup', False), rowGroup=c.get('rowGroup', False), order=c.get('order', ''), hide=c.get('hide', False), width=c.get('width', ''))
            if d := d_cols.get(c.get('field')):
                if c.get('filtered'):
                    apply_filter_js += f""" {{ {c.get('field')}: {c.get('filtered')} }}, """
                if d == 'DATE':
                    df[c['field']] = df[c['field']].apply(lambda x: x.strftime('%Y-%m-%d') if not pd.isnull(x) else '')
                    gb.configure_column(headerName=c.get('headerName', c.get('field')), field=c['field'], type='dateColumnFilter', filter=c.get('filter', ''), aggFunc=c.get('aggFunc', ''), sort=c.get('sort'), enableRowGroup=c.get('enableRowGroup', False), rowGroup=c.get('rowGroup', False), order=c.get('order', ''), hide=c.get('hide', False), width=c.get('width', ''))
    else:
        for d in df.columns:
            if d in d_cols:
                if d_cols[d] == 'SET':
                    gb.configure_column(field=d, filter='agSetColumnFilter', enableRowGroup=True)
                if d_cols[d] == 'DATE':
                    df[d] = df[d].apply(lambda x: x.strftime('%Y-%m-%d') if not pd.isnull(x) else '')
                    gb.configure_column(field=d, type='dateColumnFilter', filter=True)
                if d_cols[d] == 'NOFILTER':
                    gb.configure_column(field=d, filter=False, enableRowGroup=True)
                if d_cols[d] == 'DISTINCT':
                    gb.configure_column(field=d, filter=True, aggFunc='distinct')
                if isinstance(d_cols[d], list):
                    gb.configure_column( field=d, header_name="STATUS", filter="agSetColumnFilter", enableRowGroup=True, rowGroup=True)
                    apply_filter_js+=f""" {{ {d}: {{filterType:'set', values:['{"','".join(d_cols[d])}'] }}}} """
            elif df[d].dtype in ('int64', 'float64'):
                gb.configure_column(field=d, type='numericColumn', precision=2, filter='agNumberColumnFilter', aggFunc='sum2d', valueFormatter=decimal2)
            else:
                gb.configure_column(field=d, filter='agMultiColumnFilter')
            if d == 'WEEK_END' and report_select not in ('Target VS Staff Hours PDN',):
                gb.configure_column("YEAR", rowGroup=True, enableRowGroup=True)
                gb.configure_column("WEEK_END", rowGroup=True, enableRowGroup=True)
        if 'YEAR' in df.columns:
            df.sort_values(by=['YEAR', 'MONTH'], ascending=[False, False], inplace=True)

    # In Case you want to autosize the columns instead
    try:
        resize = report_dict[report_select].get('resize')
    except:
        resize = False
    if resize:
        column_widths = {col: df[col].astype(str).map(len).max() for col in df.columns}
        gb.configure_columns([{'headerName': col, 'field': col, 'width': max(50, column_widths[col] * 6)} for col in df.columns])
    # for col in cdefs.get(report_select, []):
    #     if col:
    #         gb.configure_column(col)

    sidebar = {
        'toolPanels': ['filters', 'columns'],
        'defaultToolPanel': ''
    }
    gb.configure_side_bar(filters_panel=True, columns_panel=True, defaultToolPanel=sidebar['toolPanels'])
    gb.configure_side_bar(sidebar)
    gb.configure_grid_options(groupDefaultExpanded=len(expanded_groups) or 1)
    grid_options = gb.build()
    grid_options['aggFuncs'] = {
        'distinct': custom_agg_distinct_js,
        'sum2d': custom_agg_sum_js
    }
    apply_filter_js = JsCode(f"""function(e) {{ e.api.setFilterModel( {apply_filter_js} )}};""")
    grid_options['onFirstDataRendered'] = apply_filter_js

    # if expanded_groups:
    #     grid_options.update({'rowGroupExpansion':{'expandedRowGroupIds': expanded_groups}})
    # Call `load_report()` with the selected report name
    left, right, = st.columns([3,1])
    with left:
        st.write(report_select)
    # with center:
    #     if st.button("Save View", key='streamlit-update-btn'):
    #         # Execute JavaScript function to click all update buttons
    #         st.markdown(click_update_button, unsafe_allow_html=True)
    with right:
        if saved_state.get(report_select):
            if st.button('Reset View'):
                saved_state[report_select] = {}
                user_cache(id, saved_state)
                st.rerun()
    response = AgGrid(df,
        gridOptions=grid_options,
        height=665,
        width='100%',
        reload_data=True,
        theme = 'streamlit',
        update_mode = GridUpdateMode.MANUAL,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        fit_columns_on_grid_load=False if resize else True,
        allow_unsafe_jscode=True,  # Set it to True to enable jsfunction
        enable_enterprise_modules=True,  # Set it to True to enable enterprise modules
        license_key='Using_this_{AG_Charts_and_AG_Grid}_Enterprise_key_{AG-076618}_in_excess_of_the_licence_granted_is_not_permitted___Please_report_misuse_to_legal@ag-grid.com___For_help_with_changing_this_key_please_contact_info@ag-grid.com___{NFTY_LLC}_is_granted_a_{Multiple_Applications}_Developer_License_for_{1}_Front-End_JavaScript_developer___All_Front-End_JavaScript_developers_need_to_be_licensed_in_addition_to_the_ones_working_with_{AG_Charts_and_AG_Grid}_Enterprise___This_key_has_been_granted_a_Deployment_License_Add-on_for_{1}_Production_Environment___This_key_works_with_{AG_Charts_and_AG_Grid}_Enterprise_versions_released_before_{6_February_2026}____[v3]_[0102]_MTc3MDMzNjAwMDAwMA==4609658b841fadff2b831dcee3a778e3',
        # custom_css = {"#gridToolBar": {"padding-bottom": "0px !important",}}
        custom_css = {"#gridToolBar": {"padding-bottom": "0px !important",},
                        ".ag-body-viewport-wrapper.ag-layout-normal": {  "overflow-x": "scroll", "overflow-y": "scroll", "padding-top":"27px"},
                        "::-webkit-scrollbar" : {"-webkit-appearance": "none","width": "8px", "height": "8px",},
                        "::-webkit-scrollbar-thumb" : {"border-radius": "4px", "background-color": "rgba(0,0,0,.4)","box-shadow": "0 0 1px rgba(255,255,255,.4)",},
                      "ag-root-wrapper":{"margin-top": "50px !important",},
                      ".ag-cell-wrapper button": {
                          "z-index": "9999",  # Ensure the button is on top
                          "position": "relative",  # Avoid overlap issues
                          "margin-top": "639px",
                          "left": "calc(100% - 62px)",
                          "color": "rgb(255,255,255,.6)",
                          'border': '0',
                          'background-color': 'rgb(0,0,0,.5)',
                      },
                      ".ag-cell-wrapper button:active, .ag-cell-wrapper button:focus": {
                               "background-color":"rgb(255,255,255,.1)"
                      },
                      }
                            )
    # Save grid state when user makes changes
    if response:
        if response.get('grid_state'):
            state = configure_grid_state(response['grid_options']['columnDefs'], response.get('grid_state') )
            if state:
                st.success(f'Locked grid view for {report_select}')
                saved_state[report_select] = state
                # save_state(STATE_FILE, saved_state)
                user_cache(id, saved_state)

def display_charts():
    nurses, nonurses, acuity = load_report('charts')
    month_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    mvals = list(month_map.values())
    # month_mmm_map = create_month_year_index()
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

def app():
    st.set_page_config(layout="wide")
    st.markdown(r"""
        <style>
            #MainMenu {visibility: show;}
            .stDeployButton {display:none;}
            footer {visibility: hidden;}
            #stDecoration {display:none;}
            }}
        </style>
    """, unsafe_allow_html=True)
    st.markdown(
        """
            <style>
                .appview-container .stMain .block-container {{
                    padding-top: {padding_top}rem;
                    padding-bottom: {padding_bottom}rem;
                    }}

            </style>""".format(
            padding_top=1, padding_bottom=1
        ),
        unsafe_allow_html=True,
    )
    cookie_manager = stx.CookieManager()
    user = cookie_manager.cookies.get('user')
    # STATE_FILE = f"cached/{user}_grid_state.json"
    if not user and 'ajs_anonymous_id' in cookie_manager.cookies:
        user = str(uuid.uuid4())
        cookie_manager.set(cookie='user',key='session_id', val=user, expires_at=datetime.datetime.now() + datetime.timedelta(days=100000))
    # st.subheader('Viva')
    # st.sidebar.title("Viva Metrics")
    if st.sidebar.button('Reset Cache'):
        load_report.clear()
    # report_select = st.sidebar.selectbox("Select Report", tuple(report_dict.keys()), )
    with st.sidebar:
        report_select = sm.option_menu("Viva Metrics", list(report_dict.keys()),
        icons=[i['icon'] for i in report_dict.values()], menu_icon="file-spreadsheet-fill", default_index=0)
    if 'chart' in report_select.lower():
        return display_charts()
    if 'upload report' in report_select.lower():
        return u_file()
    else:
        return display_report(report_select, user)

if __name__ == '__main__':
    app()
