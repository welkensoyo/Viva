import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from nfty.sflake import API as sflake_API, report_dict


@st.cache_data(ttl=60 * 60 * 24)
def load_report(report='patients_seen'):
    s = sflake_API()
    return pd.json_normalize(s.report(report))


def app():
    params = st.query_params
    st.set_page_config(layout="wide")
    st.subheader('Viva App')
    st.markdown("""
        <style>
            #MainMenu {visibility: show;}
            .stDeployButton {display:none;}
            footer {visibility: hidden;}
            #stDecoration {display:none;}
        </style>
    """, unsafe_allow_html=True)

    # Convert the list into a pandas DataFrame
    report_select = st.sidebar.selectbox(
        "Select Report",
        tuple(report_dict.keys()),
    )

    df = load_report(report_dict[report_select])
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc='sum', editable=True)

    # In Case you want to autosize the columns instead
    if params.get('resize'):
        column_widths = {col: df[col].astype(str).map(len).max() for col in df.columns}
        columnDefs = [{'headerName': col, 'field': col, 'width': max(50, column_widths[col] * 6), 'editable': True} for col in df.columns]
        gb.configure_columns(columnDefs)

    sidebar = {
        'toolPanels': ['filters', 'columns'],
        'defaultToolPanel': ''
    }
    gb.configure_side_bar(filters_panel=True, columns_panel=True, defaultToolPanel=sidebar['toolPanels'])
    gb.configure_side_bar(sidebar)
    gridOptions = gb.build()
    # Call `load_report()` with the selected report name
    st.write(report_select)
    AgGrid(df,
           gridOptions=gridOptions,
           height=600,
           width='100%',
           data_return_mode='as_input',
           update_mode='value_changed',
           fit_columns_on_grid_load=False if params.get('resize') else True,
           allow_unsafe_jscode=True,  # Set it to True to enable jsfunction
           enable_enterprise_modules=True,  # Set it to True to enable enterprise modules
           license_key='Using_this_AG_Grid_Enterprise_key_( AG-043994 )_in_excess_of_the_licence_granted_is_not_permitted___Please_report_misuse_to_( legal@ag-grid.com )___For_help_with_changing_this_key_please_contact_( info@ag-grid.com )___( Triple Play Pay )_is_granted_a_( Single Application )_Developer_License_for_the_application_( Triple Play Pay )_only_for_( 1 )_Front-End_JavaScript_developer___All_Front-End_JavaScript_developers_working_on_( Triple Play Pay )_need_to_be_licensed___( Triple Play Pay )_has_been_granted_a_Deployment_License_Add-on_for_( 1 )_Production_Environment___This_key_works_with_AG_Grid_Enterprise_versions_released_before_( 21 June 2024 )____[v2]_MTcxODkyNDQwMDAwMA==2715c856a3cb3ab5c966698c55c41fac'
           # This should be your actual ag-grid license key
           )


if __name__ == '__main__':
    app()
