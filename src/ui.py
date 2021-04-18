import PyQt5.QtWidgets as qtw
import PyQt5.QtCore as qtc
import PyQt5.QtWebEngineWidgets as qtwew
import PyQt5.QtGui as qtg
from pathlib import Path

class MainWindow (qtw.QWidget):
    def __init__(self, rdf_manager):
        super().__init__()
        self.rdf_manager = rdf_manager

        self.setWindowTitle('SPARQL-with-LA-Public-Safety-Data')
        qtg.QFontDatabase.addApplicationFont('resources/Play-Regular.ttf')

        self.setLayout(qtw.QVBoxLayout())
        self.layout().setSpacing(0)

        self.header()
        self._rdf_file_path_widgets = self.import_ui()
        self.query_ui()
        self.result_ui()
        self.output_ui()

        self.show()

    def header(self):
        container = qtw.QWidget()
        container.setLayout(qtw.QHBoxLayout())
        container.layout().setContentsMargins(0,0,0,0)

        font = qtg.QFont('Play',18)
        font.setUnderline(True)

        label = qtw.QLabel('LA Public Data Query Tool')
        label.setAlignment(qtc.Qt.AlignCenter)
        label.setFont(font)
        container.layout().addWidget(label)

        self.layout().addWidget(container, stretch = 10)
    
    def import_ui(self):
        container = qtw.QWidget()
        container.setLayout(qtw.QHBoxLayout())
        container.layout().setContentsMargins(0,0,0,0)
        font = qtg.QFont('Play', 12)

        label = qtw.QLabel('RDF File Path:')
        label.setFont(font)
        container.layout().addWidget(label) 

        textbox = qtw.QLineEdit()
        textbox.setObjectName('filename')
        textbox.setFont(font)
        textbox.textEdited.connect(lambda _: textbox.setStyleSheet(''))
        container.layout().addWidget(textbox) 

        buttom = qtw.QPushButton('Import')
        buttom.setFont(font)
        buttom.clicked.connect(self.import_button_clicked)
        container.layout().addWidget(buttom) 

        self.layout().addWidget(container, stretch = 2)

    def query_ui(self):
        container = qtw.QWidget()
        container.setLayout(qtw.QGridLayout())
        container.layout().setContentsMargins(0,0,0,0)
        container.layout().setVerticalSpacing(0)

        font = qtg.QFont('Play', 12)
        
        label = qtw.QLabel('Query:')
        label.setFont(font)
        container.layout().addWidget(label, 0, 0, 2, 1)

        textbox = qtw.QPlainTextEdit()
        textbox.setObjectName('sparql-query')
        textbox.setFont(font)
        container.layout().addWidget(textbox, 0, 1, 2, 1)

        buttom_0 = qtw.QPushButton('Search')
        buttom_0.setFont(font)
        sp = buttom_0.sizePolicy()
        sp.setVerticalPolicy(qtw.QSizePolicy.Policy.Expanding)
        buttom_0.setSizePolicy(sp)
        container.layout().addWidget(buttom_0, 0, 2, 1, 1)
        buttom_0.clicked.connect(self.search_button_clicked)

        buttom_1 = qtw.QPushButton('To HTML')
        buttom_1.setObjectName('to-html')
        buttom_1.setFont(font)
        sp = buttom_1.sizePolicy()
        sp.setVerticalPolicy(qtw.QSizePolicy.Policy.Expanding)
        buttom_1.setSizePolicy(sp)
        container.layout().addWidget(buttom_1, 1, 2, 1, 1)
        buttom_1.clicked.connect(self.to_html_button_clicked)
        buttom_1.setHidden(True)

        buttom_2 = qtw.QPushButton('To Plaintext')
        buttom_2.setObjectName('to-plaintext')
        buttom_2.setFont(font)
        sp = buttom_2.sizePolicy()
        sp.setVerticalPolicy(qtw.QSizePolicy.Policy.Expanding)
        buttom_2.setSizePolicy(sp)
        container.layout().addWidget(buttom_2, 1, 2, 1, 1)
        buttom_2.clicked.connect(self.to_plain_text_button_clicked)
        

        self.layout().addWidget(container, stretch = 15)

    def result_ui(self):
        container = qtw.QWidget()
        container.setLayout(qtw.QHBoxLayout())
        container.layout().setContentsMargins(0,0,0,0)
        font = qtg.QFont('Play', 10)


        label = qtw.QLabel('18746 results found')
        label.setObjectName('count-output')
        label.setFont(font)
        label.setAlignment(qtc.Qt.AlignCenter)
        container.layout().addWidget(label)

        self.layout().addWidget(container, stretch = 1)

    def output_ui(self):
        container = qtw.QWidget()
        container.setLayout(qtw.QHBoxLayout())
        container.layout().setContentsMargins(0,0,0,0)

        browser = qtwew.QWebEngineView()
        browser.setObjectName('html-output')
        browser.setHtml('''<!DOCTYPE html><html><body> <h1>My First Heading</h1><p>My first paragraph.</p></body></html>''')
        container.layout().addWidget(browser)

        self.layout().addWidget(browser, stretch = 100)

    def import_button_clicked(self):
        if self.findChild(qtw.QLineEdit, 'filename').text() == '':
            filename, _ = qtw.QFileDialog.getOpenFileName(None, 'Open File', 'C:\\','*.*')
        else:
            filename = self.findChild(qtw.QLineEdit, 'filename').text()
        
        try:
            path = Path(filename).resolve()
            if path.exists():
                self.findChild(qtw.QLineEdit, 'filename').setText(str(path))

            else:
                raise Exception()
        except:
            self.findChild(qtw.QLineEdit, 'filename').setStyleSheet('border: 1px solid red')
    
    def search_button_clicked(self):
        if not self.findChild(qtw.QPushButton, 'to-html').isHidden():
            self.to_html_button_clicked()
        query = self.findChild(qtw.QPlainTextEdit, 'sparql-query').toPlainText()
        
    def to_html_button_clicked(self):
        self.findChild(qtw.QPushButton, 'to-html').setHidden(True)
        self.findChild(qtw.QPushButton, 'to-plaintext').setHidden(False)
        qwebview = self.findChild(qtwew.QWebEngineView, 'html-output')
        qwebview.page().toPlainText( lambda html: qwebview.page().setHtml(html))

    def to_plain_text_button_clicked(self):
        self.findChild(qtw.QPushButton, 'to-html').setHidden(False)
        self.findChild(qtw.QPushButton, 'to-plaintext').setHidden(True)

        qwebview = self.findChild(qtwew.QWebEngineView, 'html-output')
        qwebview.page().toHtml( lambda html: qwebview.page().setContent(qtc.QByteArray(html.encode())))



