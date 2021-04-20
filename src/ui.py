import dominate
from dominate.tags import *
from pathlib import Path
import PyQt5.QtGui as qtg
import PyQt5.QtCore as qtc
import PyQt5.QtWidgets as qtw
import PyQt5.QtWebEngineCore as qtwec
import PyQt5.QtWebEngineWidgets as qtwew
import re
from urllib.parse import quote, unquote

class MainWindow (qtw.QWidget):
    """The MainWindow class used to initialize ui and its functionality. 

    Args:
        PyQt5.QtWidgets.QWdiget: the base class for all user interface objects in PyQt5.
    """
    def __init__(self, rdf_manager, scheme_handler):
        """Initialize main window. 

        Args:
            rdf_manager (rdf.Manager): the rdf manager used to manage rdf graphs.
            scheme_handler (PyQt5.QtWebEngineCore.QWebEngineUrlSchemeHandler): a handler used to handle custom url requests.
        """
        #Initialize parents class
        super().__init__()

        #Initialize variables
        self.rdf_manager = rdf_manager
        self.scheme_handler = scheme_handler
        self.chunked_data=[[[]]]

        #Initialize main window
        self.setWindowTitle('SPARQL-with-LA-Public-Safety-Data')
        qtg.QFontDatabase.addApplicationFont('resources/Play-Regular.ttf')
        self.setLayout(qtw.QVBoxLayout())
        self.layout().setSpacing(0)

        #Initlaize the rest of the ui
        self.title_component()
        self._rdf_file_path_widgets = self.import_component()
        self.query_component()
        self.count_and_chunk_selector_component()
        self.output_component()

        #Show main window
        self.show()

    def title_component(self):
        """Initalize title component.
        """
        #Create the container to store all child components
        container = qtw.QWidget()
        container.setLayout(qtw.QHBoxLayout())
        container.layout().setContentsMargins(2, 2, 2, 2)

        #Initialize custom font
        font = qtg.QFont('Play',18)
        font.setUnderline(True)

        #Create child components
        label = qtw.QLabel('LA Public Data Query Tool')
        label.setAlignment(qtc.Qt.AlignCenter)
        label.setFont(font)
        container.layout().addWidget(label)

        #Add title components to main window
        self.layout().addWidget(container, stretch = 10)
    
    def import_component(self):
        """Initialize import component.
        """
        #Create containers to store all child componenets 
        container = qtw.QWidget()
        container.setLayout(qtw.QHBoxLayout())
        container.layout().setContentsMargins(2, 2, 2, 2)

        #Initialize custom font
        font = qtg.QFont('Play', 12)

        #Create child componenets 
        #label component
        label = qtw.QLabel('RDF File Path:')
        label.setFont(font)
        container.layout().addWidget(label) 

        #filename input field component
        textbox = qtw.QLineEdit()
        textbox.setObjectName('filename')
        textbox.setFont(font)
        textbox.textEdited.connect(lambda _: textbox.setStyleSheet(''))
        container.layout().addWidget(textbox) 

        #Import button
        buttom = qtw.QPushButton('Import')
        buttom.setFont(font)
        buttom.clicked.connect(self.import_button_clicked)
        container.layout().addWidget(buttom) 

        #Add import component to main window
        self.layout().addWidget(container, stretch = 2)

    def query_component(self):
        """Initialize query component.
        """
        #Create container to store all child components
        container = qtw.QWidget()
        container.setLayout(qtw.QGridLayout())
        container.layout().setContentsMargins(2, 2, 2, 2)
        container.layout().setVerticalSpacing(0)

        #Initialize custom font
        font = qtg.QFont('Play', 12)
        
        #Create child components
        #Label component
        label = qtw.QLabel('Query:')
        label.setFont(font)
        container.layout().addWidget(label, 0, 0, 2, 1)

        #SPARQL input field component
        textbox = qtw.QPlainTextEdit()
        textbox.setObjectName('sparql-query')
        textbox.setFont(font)
        container.layout().addWidget(textbox, 0, 1, 2, 1)

        #Search button component
        buttom_0 = qtw.QPushButton('Search')
        buttom_0.setFont(font)
        sp = buttom_0.sizePolicy()
        sp.setVerticalPolicy(qtw.QSizePolicy.Policy.Expanding)
        buttom_0.setSizePolicy(sp)
        container.layout().addWidget(buttom_0, 0, 2, 1, 1)
        buttom_0.clicked.connect(self.search_button_clicked)

        #To HTML button component
        buttom_1 = qtw.QPushButton('To HTML')
        buttom_1.setObjectName('to-html')
        buttom_1.setFont(font)
        sp = buttom_1.sizePolicy()
        sp.setVerticalPolicy(qtw.QSizePolicy.Policy.Expanding)
        buttom_1.setSizePolicy(sp)
        container.layout().addWidget(buttom_1, 1, 2, 1, 1)
        buttom_1.clicked.connect(self.to_html_button_clicked)
        buttom_1.setHidden(True)

        #To Text button component
        buttom_2 = qtw.QPushButton('To Text')
        buttom_2.setObjectName('to-plaintext')
        buttom_2.setFont(font)
        sp = buttom_2.sizePolicy()
        sp.setVerticalPolicy(qtw.QSizePolicy.Policy.Expanding)
        buttom_2.setSizePolicy(sp)
        container.layout().addWidget(buttom_2, 1, 2, 1, 1)
        buttom_2.clicked.connect(self.to_plain_text_button_clicked)
        
        #Add query component to main window
        self.layout().addWidget(container, stretch = 15)

    def count_and_chunk_selector_component(self):
        """Initialize result count and result display chunk selector component.
        """
        #Create container to store all child components
        container = qtw.QWidget()
        container.setLayout(qtw.QHBoxLayout())
        container.layout().setContentsMargins(2, 2, 2, 2)

        #Initalize custom fonts
        font = qtg.QFont('Play', 10)

        #Create child components
        #Result count component
        label = qtw.QLabel('')
        label.setObjectName('count-output')
        label.setFont(font)
        label.setAlignment(qtc.Qt.AlignCenter)
        sp = label.sizePolicy()
        sp.setHorizontalPolicy(qtw.QSizePolicy.Policy.Expanding)
        label.setSizePolicy(sp)
        container.layout().addWidget(label, 22)

        #Label component
        label2 = qtw.QLabel('Chunk: ')
        label2.setFont(font)
        label2.setAlignment(qtc.Qt.AlignCenter)
        sp = label2.sizePolicy()
        sp.setHorizontalPolicy(qtw.QSizePolicy.Policy.Fixed)
        label2.setSizePolicy(sp)
        container.layout().addWidget(label2, 22)

        #Chunk Selector component
        combo_box = qtw.QComboBox()
        combo_box.setObjectName('chunk-selector')
        combo_box.setFont(font)
        combo_box.setEditable(False)
        sp = combo_box.sizePolicy()
        sp.setHorizontalPolicy(qtw.QSizePolicy.Policy.Fixed)
        combo_box.setSizePolicy(sp)
        combo_box.currentIndexChanged.connect(self.chunk_selection_change)
        container.layout().addWidget(combo_box, 1)

        #Add child components to main window
        self.layout().addWidget(container, stretch = 1)

    def output_component(self):
        """Initialize output_component.
        """
        #Create container to store all child components
        container = qtw.QWidget()
        container.setLayout(qtw.QHBoxLayout())
        container.layout().setContentsMargins(2, 2, 2, 2)

        #Create child components
        #Web Viwer component 
        browser = qtwew.QWebEngineView()
        browser.setObjectName('html-output')
        browser.setContextMenuPolicy(qtc.Qt.ContextMenuPolicy.NoContextMenu)
        container.layout().addWidget(browser)

        #Add output component to main window
        self.layout().addWidget(browser, stretch = 100)

    def import_button_clicked(self):
        """Execute when import button is clicked.
        """
        #Check if user provides path
        #If not path is provided by user, show file selector
        if self.findChild(qtw.QLineEdit, 'filename').text() == '':
            filename, _ = qtw.QFileDialog.getOpenFileName(None, 'Open File', './','*.*')
        #Else, retreive user provided path
        else:
            filename = self.findChild(qtw.QLineEdit, 'filename').text()

        #Attempt to import rdf file to graph
        successfully_imported, path = self.rdf_manager.import_file(filename)

        #Update the ui to show which file is being imported
        self.findChild(qtw.QLineEdit, 'filename').setText(str(path))

        #If import successfully, show green border around filename input field
        if successfully_imported:
            self.findChild(qtw.QLineEdit, 'filename').setStyleSheet('border: 2px solid LightGreen')
        #Else, show  red border
        else:
            self.findChild(qtw.QLineEdit, 'filename').setStyleSheet('border: 2px solid red')
    
    def search_button_clicked(self):
        """Execute when search button is clicked.
        """
        #Retreive the query from sparql input field component
        query = self.findChild(qtw.QPlainTextEdit, 'sparql-query').toPlainText()

        self.excute_query_process(query)
        
        
    def excute_query_process(self, query):
        """Execute query and update ui to reflex the new changes.

        Args:
            query (string): a sparql statment used to query  the graph.
        """
        #Query graph
        result = self.rdf_manager.query(query)

        #Extract headers from SPARQL query. Use 1,2,3... if no header is provided.
        splitted_query = re.split('where', query, flags=re.IGNORECASE)[0]

        splitted_query = re.split(' ', splitted_query)[1:]
        splitted_query = [x for x in splitted_query if x !='']

        headers = []
        accumulation = ''

        if '*' in re.split('where', query, flags=re.IGNORECASE)[0]:
            headers = [x for x in range(len(result[0]))]
        else:
            for i in range(len(splitted_query)):
                accumulation = accumulation + ' ' + splitted_query[i]
                if (accumulation.count('(') == accumulation.count (')')):
                    match = re.compile(r"[?][\S]+$")
                    accumulation = match.search(accumulation).group(0)
                    if accumulation[-1] == ')':
                        accumulation = accumulation[:-1]
                    
                    headers.append(accumulation)     

        #Insert headers onto result
        result.insert(0,headers)

        #Show how many result found
        self.findChild(qtw.QLabel, 'count-output').setText( str(len(result)-1) + ' results found')

        #Split result into chuck 
        self.chunked_data = self.split_data_into_chunk(result)

        #Update chunk selector options
        chunk_selector = self.findChild(qtw.QComboBox, 'chunk-selector')
        chunk_selector.clear()
        chuck_indexes = range(len(self.chunked_data))
        chuck_indexes = [str(x) for x in chuck_indexes]
        chunk_selector.addItems(chuck_indexes)

        #Show chunk 0 
        self.chunk_selection_change(0)


    def to_html_button_clicked(self):
        
        """Execute when To HTML button is clicked.
        """
        #Disable To HTML button
        self.findChild(qtw.QPushButton, 'to-html').setHidden(True)

        #Enable To Text button
        self.findChild(qtw.QPushButton, 'to-plaintext').setHidden(False)

        #Update web viwer to render data
        self.update_web_viewer(_type='html')

    def to_plain_text_button_clicked(self):
        """Execute when To Text button is clicked.
        """
        #Enable To HTML button
        self.findChild(qtw.QPushButton, 'to-html').setHidden(False)

        #Disable To Text button
        self.findChild(qtw.QPushButton, 'to-plaintext').setHidden(True)

        #Update web viewr to not render data
        self.update_web_viewer(_type='plain')

    def update_web_viewer(self, data = None, _type = None):
        """Update web viewer by modifying how custom url handler respond to request.

        Args:
            data (string, optional): data stored by the custom url handler. Defaults to None.
            _type (string, optional): reply format of data by the custom url handler. Defaults to None.
        """
        #If data is provided, update handler stored ata
        if data:
            self.scheme_handler.set_data(data)

        #If type is provided, update handler stored type
        if _type:
            self.scheme_handler.set_type(_type)
        
        #Reset web viwer and load new resppond
        self.findChild(qtwew.QWebEngineView, 'html-output').setHtml('')
        self.findChild(qtwew.QWebEngineView, 'html-output').load(qtc.QUrl('custom-url-scheme://retrieve-data/'))

    def chunk_selection_change(self, index):
        """Execute when result chuck selection is changed.

        Args:
            index (int): new index of the chuck selected.
        """
        #Reset to To HTML button if it is still at To Text button
        if not self.findChild(qtw.QPushButton, 'to-html').isHidden():
            self.to_html_button_clicked()

        #Convert data chuck to html table
        html = self.data_to_html(self.chunked_data[index])

        #Update web viewer to show the latest result
        self.update_web_viewer(data = html, _type='html')

    def data_to_html(self, data):
        """Convert 2d array of data to html table. Ensure that all urls are hyperlink.

        Args:
            data (string): 2d array contains data to be converted.

        Returns:
            string: html table contains data.
        """
        #Create html document
        doc = dominate.document(title='SPARQL-Results')

        #Initalize document style
        with doc.head:
            style('''
            table, th, td {
                border: 1px solid black;
            }
            
            ''')

        #Add children to html documents
        with doc.add(table()):
            with thead().add(tr()):
                for i in data[0]:
                    th(i)
            with tbody():
                for i in range(1, len(data)):
                    with tr():
                        for j in data[i]:
                            if 'https://' in j or 'http://' in j:
                                with td():
                                    #Replace namepsace with prefix
                                    shorted_name = j
                                    for prefix_namespace in self.rdf_manager.get_namespace():
                                        prefix, namespace = prefix_namespace
                                        prefix = str(prefix)+':'
                                        namespace = str(namespace)
                                        if namespace in j:
                                            shorted_name = j.replace(namespace, prefix)

                                    node = a(shorted_name)
                                    node['href']='custom-url-scheme://redirect-to/' + quote('<'+ j + '>') + '/'
                            else:
                                td(j)
        return (str(doc))
    
    def split_data_into_chunk (self, data, chunk_size = 1000):
        """Split data into smaller chunk.

        Args:
            data (string): data to be split.
            chunk_size (int, optional): size of each chunk. Defaults to 1000.

        Returns:
            string: a list of chucked data.
        """
        #Do nothing if there is no data
        if len(data) <=1:
            return [data]

        #Extract the header from data
        header = data[0]
        data = data[1:]

        #Store all data chunk
        chucked_data = []

        #Start chunking data and add header to each chunk
        for i in range(0,len(data), chunk_size):
            temp_data = data[i:i+chunk_size]
            temp_data.insert(0, header)
            chucked_data.append(temp_data)

        return chucked_data


class SchemeHandler (qtwec.QWebEngineUrlSchemeHandler):
    """A handler used to respond to the custom url requests.

    Args:
        PyQt5.QtWebEngineCore.QWebEngineUrlSchemeHandler: abstract class used to handle custom url.
    """
    def __init__(self, parent=None):
        """Initalize the handler.

        Args:
            parent (QtCore.QObject, optional): parent object of this handler. Defaults to None.
        """
        super().__init__(parent)
        self._data = ''
        self._type = ''
        self._ui=None

    def requestStarted(self, job):
        """Execute when http occured.

        Args:
            job (PyQt5.QtWebEngineCore.QWebEngineUrlRequestJob): the object contains all information related to the request.
        """
        request_url = job.requestUrl().toString()
        if request_url =='custom-url-scheme://retrieve-data/':
            self._request_to_retrieve_data(job)
        else:
            target = request_url[32: len(request_url)-1]
            target = unquote(target)
            self._rediect_to_data(job, target)
            pass

    def _rediect_to_data(self, job, target):
        """Tell main window to execute a query for new target

        Args:
            job (PyQt5.QtWebEngineCore.QWebEngineUrlRequestJob): the object contains all information related to the request.
            target (string): the reference that should be direct to
        """
        query0 = 'SELECT (COALESCE(%s) as ?s) ?p ?o WHERE {%s ?p ?o}' % (target, target)
        query1 = 'SELECT ?s (COALESCE(%s) as ?p) ?o WHERE {?s %s ?o}' % (target, target)
        query2 = 'SELECT ?s ?p (COALESCE(%s) as ?o) WHERE {?s ?p %s}' % (target, target)

        query = 'SELECT ?s ?p ?o WHERE {{%s} UNION {%s} UNION {%s}}' % (query0, query1, query2)

        self._ui.excute_query_process(query)
        pass


    def _request_to_retrieve_data(self, job):
        """Execute this function when a request to retreive data is recevied

        Args:
            job (PyQt5.QtWebEngineCore.QWebEngineUrlRequestJob): the object contains all information related to the request.
        """
        #Create buff to store data
        buff = qtc.QBuffer(parent=self)
        buff.open(qtc.QIODevice.WriteOnly)
        buff.write(self._data.encode())
        buff.seek(0)
        buff.close()

        #Replay to the request with data and its type
        job.reply(self._type, buff)

    
    def set_data(self, data):
        """Modify data stored in this handler.

        Args:
            data (string): data to be stored.
        """
        self._data = data
    
    def set_type(self, type_):
        """Modify data type stored in this handler.

        Args:
            type_ (string): data type to be stored.
        """
        self._type = ('text/'+ type_).encode()

    def set_ui(self, ui):
        """Provide reference of ui to the handler.

        Args:
            ui (MainWindow): reference of ui.
        """
        self._ui = ui

def install_scheme_handler():
    """Initlaize custom url scheme and register it with a handler .

    Returns:
        handler: the handler responsibles for responding to custom url requests .
    """
    #initlaize url scheme
    scheme =  qtwec.QWebEngineUrlScheme(b'custom-url-scheme')
    scheme.setSyntax(qtwec.QWebEngineUrlScheme.Syntax.HostAndPort)
    scheme.setDefaultPort(2345)
    scheme.setFlags(qtwec.QWebEngineUrlScheme.Flag.SecureScheme) 
    qtwec.QWebEngineUrlScheme.registerScheme(scheme)

    #Register url scheme and handler together
    handler = SchemeHandler(qtw.QApplication.instance())
    qtwew.QWebEngineProfile.defaultProfile().installUrlSchemeHandler(b'custom-url-scheme', handler)
    
    return handler        