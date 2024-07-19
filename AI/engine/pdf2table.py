from llama_index.core.tools import FunctionTool
from img2table.document import PDF


def engine(file):
    print(file)
    with open(file, 'rb') as f:
        pdf_bytes = f.read()
    return PDF(src=pdf_bytes)

description = 'this tool can convert a supplied pdf file to table via ocr, make sure to use the full path to file: {}'

pdf2table_engine = FunctionTool.from_defaults(
    fn=engine,
    name="pdf_to_table",
    description="this tool can convert a supplied pdf file to table via ocr, make sure to use the full path to file",
)
