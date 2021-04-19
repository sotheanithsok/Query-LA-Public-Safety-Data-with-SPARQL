from src.ui import MainWindow, install_scheme_handler
from src.rdf import Manager
from PyQt5.QtWidgets import QApplication

#Create QApplication
app =QApplication([])

#Register the handlers
handler = install_scheme_handler()

#Initlaize main window
mw = MainWindow(Manager(), handler)

#Show QApplication
app.exec()