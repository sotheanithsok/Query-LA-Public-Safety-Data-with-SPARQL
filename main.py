from src.ui import MainWindow
from src.rdf import Manager
from PyQt5.QtWidgets import QApplication


app =QApplication([])
mw = MainWindow(Manager())
app.exec()
