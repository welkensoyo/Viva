from st_aggrid import JsCode
from pprint import pprint

def configure_grid_state(options, state):
    # pprint(state)
    keys = (('aggregation', 'aggregationModel'), ('columnSizing', 'columnSizingModel'), ('sort', 'sortModel'))
    groups = state.get('rowGroup',{}).get('groupColIds',[])
    order = state.get('columnOrder',{}).get('orderedColIds',[])
    hidden = state.get('columnVisibility', {}).get('hiddenColIds', [])
    filtered = state.get('filter',{}).get('filterModel',{})
    fields = {}
    for k in keys:
        if a:= state.get(k[0]):
            a = a[k[1]]
            for c in a:
                col = c.pop('colId','')
                try:
                    fields[col].update(c)
                except KeyError:
                    fields[col] = c

    for c in options:
        c['sort'] = ''
        if c['field'] in fields:
            c.update(fields[c['field']])
            if order:
                c['order'] = order.index(c['field'])
        if c['field'] in groups:
            c['rowGroup'] = True
        else:
            c['rowGroup'] = False
        if c['field'] in hidden:
            c['hide'] = True
        else:
            c['hide'] = False
        if c['field'] in filtered:
            c['filtered'] = filtered[c['field']]
    options = sorted(options, key=lambda x: x['order'])
    options.append(state.get('rowGroupExpansion',{}).get('expandedRowGroupIds',[]))
    return options

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

decimal2 = JsCode("""
        function(params) {
            if (params.value === null || params.value === undefined) {
                return '';
            }
            const value = Number(params.value);
            return Number.isInteger(value) ? value : value.toFixed(2);
        }
    """)

set_state_js = JsCode(f"""
function setState(stateString) {{
    try {{
        const state = JSON.parse(stateString);
        gridOptions.api.setFilterModel(state.filterModel);
        gridOptions.columnApi.applyColumnState({{
            state: state.columnState,
            applyOrder: true,
        }});
        console.log("Applied state:", state);
    }} catch (error) {{
        console.error("Failed to set state:", error);
    }}
}}
""")

custom_css = """
<style>
.ag-cell-wrapper button {
    background-color: #4CAF50; /* Green background */
    border: none;              /* Remove borders */
    color: white;              /* White text */
    padding: 10px 20px;        /* Padding */
    text-align: center;        /* Centered text */
    text-decoration: none;     /* Remove underline */
    display: inline-block;     /* Display as inline-block */
    font-size: 16px;           /* Button font size */
    margin: 4px 2px;           /* Some margin */
    cursor: pointer;           /* Pointer/hand icon */
    border-radius: 12px;       /* Rounded corners */
}

.ag-cell-wrapper button:hover {
    background-color: #45a049; /* Darker green on hover */
}
</style>
"""
