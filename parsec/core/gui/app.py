import time

from PyQt5.QtCore import QSettings
from PyQt5.QtWidgets import QApplication, QSplashScreen
from PyQt5.QtGui import QPixmap

from parsec.core.gui import lang
from parsec.core.gui.core_call import init_core_call
from parsec.core.gui.main_window import MainWindow
from parsec.core.gui.core_call import core_call


def run_gui(parsec_core, trio_portal, cancel_scope):
    print("Starting UI")

    app = QApplication([])
    app.setOrganizationName('Scille')
    app.setOrganizationDomain('parsec.cloud')
    app.setApplicationName('Parsec')

    # splash = QSplashScreen(QPixmap(':/logos/images/logos/parsec.png'))
    # splash.show()
    # app.processEvents()

    init_core_call(parsec_core=parsec_core, trio_portal=trio_portal,
                   cancel_scope=cancel_scope)

    lang.switch_to_locale()

    win = MainWindow()
    win.show()
    # splash.finish(win)

    return app.exec_()
