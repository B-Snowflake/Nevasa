#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2024/7/16

import math
import geopandas
from BlurWindow.blurWindow import GlobalBlur
from matplotlib.figure import Figure
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Signal
from PySide6.QtGui import Qt, QIcon, QPixmap
from PySide6.QtWidgets import QVBoxLayout, QTableView, QWidget, QMainWindow, QPushButton, QHBoxLayout, QSpacerItem, QSizePolicy, QLineEdit, QLabel
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas

# matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体 (SimHei) 字体


class ReadGeodata(QMainWindow):
    closed = Signal()

    def __init__(self, engine, is_transparent_window, filename=None, postgis_con=None, table_name=None, geom_col=None):
        if filename is None and postgis_con is None:
            raise Exception('both filename and postgis connection is None')
        if postgis_con is not None and (table_name is None or geom_col is None):
            raise Exception('need table_name and geometry_column')
        super().__init__()
        self.icon = QIcon()
        self.icon.addPixmap(QPixmap("resources/icon/Nevasa.ico").scaled(64, 64), QIcon.Mode.Normal, QIcon.State.Off)
        self.setWindowIcon(self.icon)
        self.current_page = 1
        self.rows_per_page = 15
        self.engine = engine
        self.is_transparent_window = is_transparent_window
        self.is_transparent_window = False
        self.selected_data = None
        self.filename = filename
        self.postgis_con = postgis_con
        self.table_name = table_name
        self.geom_col = geom_col
        self.read_geodata()
        if self.is_transparent_window:
            GlobalBlur(self.winId(), Dark=True, Acrylic=False, QWidget=self)
        else:
            self.setStyleSheet("background-color: rgb(243, 243, 243);")
        if engine.upper() == 'FILE':
            self.page_num = math.ceil(len(self.geodata) / self.rows_per_page)
            self.data_count = len(self.geodata)
        elif engine.upper() == 'POSTGIS':
            self.get_page_num_from_postgis()

    def setupui(self):
        # 创建表格模型并设置为 QTableView 的数据源
        model = PandasModel(self.display_geodata.iloc[0:self.rows_per_page])
        # 创建 QTableView 控件并设置模型
        self.table_view = QTableView()
        self.table_view.setWordWrap(False)
        self.table_view.setModel(model)
        self.table_view.verticalHeader().hide()
        self.set_tableview_stylesheet(self.table_view)
        self.geodata_preview_canvas = PlotCanvas(parent=self)
        self.geodata_preview_canvas.setMaximumWidth(450)
        # 创建分页按钮
        self.prev_button = QPushButton("上一页")
        self.set_pushbutton_stylesheet(self.prev_button)
        self.prev_button.setMaximumWidth(50)
        self.prev_button.setEnabled(False)
        self.next_button = QPushButton("下一页")
        self.set_pushbutton_stylesheet(self.next_button)
        self.next_button.setMaximumWidth(50)
        # 按钮点击事件绑定
        self.prev_button.clicked.connect(self.previous_page)
        self.next_button.clicked.connect(self.next_page)
        self.page_lineedit = QLineEdit()
        self.set_lineedit_stylesheet(self.page_lineedit)
        self.page_lineedit.textChanged.connect(self.page_lineedit_input_validator)
        self.page_lineedit.setMaximumWidth(50)
        self.page_lineedit.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.jump_page_button = QPushButton(text='跳转')
        self.set_pushbutton_stylesheet(self.jump_page_button)
        self.jump_page_button.setMaximumWidth(50)
        self.jump_page_button.setMinimumWidth(40)
        self.jump_page_button.clicked.connect(self.to_page)
        # 设置布局
        self.main_layout = QHBoxLayout()
        self.v_layout = QVBoxLayout()
        self.h_layout = QHBoxLayout()
        self.h_layout.setSpacing(20)
        self.add_page_button()
        self.v_layout.addWidget(self.table_view)
        self.v_layout.addLayout(self.h_layout)
        self.main_layout.addLayout(self.v_layout)
        self.main_layout.addWidget(self.geodata_preview_canvas)
        # 设置主窗口
        container = QWidget()
        container.setLayout(self.main_layout)
        self.setCentralWidget(container)
        self.setWindowTitle("矢量数据预览")
        self.setFixedSize(1200, 540)
        self.table_view.selectionModel().selectionChanged.connect(self.on_selection_changed)

    def page_lineedit_input_validator(self):
        try:
            if int(self.page_lineedit.text()) > self.page_num:
                self.page_lineedit.setText(str(self.page_num))
            elif int(self.page_lineedit.text()) < 1:
                self.page_lineedit.setText('1')
        except:
            pass

    def add_page_button(self):
        self.page_buttons = []
        self.h_layout_spaceritem_left = QSpacerItem(20, 0, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.h_layout.addItem(self.h_layout_spaceritem_left)
        self.h_layout.addWidget(self.prev_button)
        if self.page_num <= 9:
            for i in range(1, self.page_num + 1):
                page_button = QPushButton(str(i))
                if i == self.current_page:
                    # page_button.setStyleSheet("background-color: #90EE90;")
                    page_button.setEnabled(False)
                page_button.setMinimumWidth(30)
                page_button.setMaximumWidth(30)
                page_button.clicked.connect(self.to_page)
                self.set_pushbutton_stylesheet(page_button)
                self.page_buttons.append(page_button)
                self.h_layout.addWidget(page_button)
        else:
            if self.current_page <= 4:
                for i in range(1, max(7, self.current_page + 2)):
                    page_button = QPushButton(str(i))
                    if i == self.current_page:
                        # page_button.setStyleSheet("background-color: #90EE90;")
                        page_button.setEnabled(False)
                    page_button.setMinimumWidth(30)
                    page_button.setMaximumWidth(30)
                    page_button.clicked.connect(self.to_page)
                    self.set_pushbutton_stylesheet(page_button)
                    self.page_buttons.append(page_button)
                    self.h_layout.addWidget(page_button)
                page_label = QLabel(text='...')
                self.set_label_stylesheet(page_label)
                self.page_buttons.append(page_label)
                page_label.setMaximumWidth(30)
                self.h_layout.addWidget(page_label)
                page_button = QPushButton(str(self.page_num))
                page_button.setMinimumWidth(30)
                page_button.setMaximumWidth(30)
                page_button.clicked.connect(self.to_page)
                self.set_pushbutton_stylesheet(page_button)
                self.page_buttons.append(page_button)
                self.h_layout.addWidget(page_button)
            elif self.current_page > 3 and abs(self.current_page - self.page_num) > 3:
                page_button = QPushButton('1')
                page_button.setMinimumWidth(30)
                page_button.setMaximumWidth(30)
                page_button.clicked.connect(self.to_page)
                self.page_buttons.append(page_button)
                self.h_layout.addWidget(page_button)
                page_label = QLabel(text='...')
                self.set_label_stylesheet(page_label)
                page_label.setMaximumWidth(30)
                self.set_pushbutton_stylesheet(page_button)
                self.page_buttons.append(page_label)
                self.h_layout.addWidget(page_label)
                for i in range(max(1, self.current_page - 2), min(self.page_num, self.current_page + 3)):
                    page_button = QPushButton(str(i))
                    if i == self.current_page:
                        # page_button.setStyleSheet("background-color: #90EE90;")
                        page_button.setEnabled(False)
                    page_button.setMinimumWidth(30)
                    page_button.setMaximumWidth(30)
                    page_button.clicked.connect(self.to_page)
                    self.set_pushbutton_stylesheet(page_button)
                    self.page_buttons.append(page_button)
                    self.h_layout.addWidget(page_button)
                page_label = QLabel(text='...')
                self.set_label_stylesheet(page_label)
                page_label.setMaximumWidth(30)
                self.page_buttons.append(page_label)
                self.h_layout.addWidget(page_label)
                page_button = QPushButton(str(self.page_num))
                page_button.setMaximumWidth(30)
                page_button.setMinimumWidth(30)
                page_button.clicked.connect(self.to_page)
                self.set_pushbutton_stylesheet(page_button)
                self.page_buttons.append(page_button)
                self.h_layout.addWidget(page_button)
            elif self.current_page > 3 >= abs(self.current_page - self.page_num):
                page_button = QPushButton('1')
                page_button.setMinimumWidth(30)
                page_button.setMaximumWidth(30)
                page_button.clicked.connect(self.to_page)
                self.set_pushbutton_stylesheet(page_button)
                self.page_buttons.append(page_button)
                self.h_layout.addWidget(page_button)
                page_label = QLabel(text='...')
                self.set_label_stylesheet(page_label)
                self.page_buttons.append(page_label)
                page_label.setMaximumWidth(30)
                self.h_layout.addWidget(page_label)
                for i in range(self.page_num - 6, min(self.page_num, self.current_page + 3)):
                    page_button = QPushButton(str(i))
                    if i == self.current_page:
                        # page_button.setStyleSheet("background-color: #90EE90;")
                        page_button.setEnabled(False)
                    page_button.setMinimumWidth(30)
                    page_button.setMaximumWidth(30)
                    page_button.clicked.connect(self.to_page)
                    self.set_pushbutton_stylesheet(page_button)
                    self.page_buttons.append(page_button)
                    self.h_layout.addWidget(page_button)
                page_button = QPushButton(str(self.page_num))
                if self.current_page == self.page_num:
                    # page_button.setStyleSheet("background-color: #90EE90;")
                    page_button.setEnabled(False)
                page_button.setMinimumWidth(30)
                page_button.setMaximumWidth(30)
                page_button.clicked.connect(self.to_page)
                self.set_pushbutton_stylesheet(page_button)
                self.page_buttons.append(page_button)
                self.h_layout.addWidget(page_button)
        self.h_layout.addWidget(self.next_button)
        self.h_layout.addWidget(self.page_lineedit)
        self.h_layout.addWidget(self.jump_page_button)
        self.h_layout_spaceritem_right = QSpacerItem(20, 0, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.h_layout.addItem(self.h_layout_spaceritem_right)

    def to_page(self):
        if self.sender() == self.jump_page_button:
            self.current_page = int(self.page_lineedit.text())
            self.update_table()
        else:
            self.current_page = int(self.sender().text())
            self.update_table()

    def update_table(self):
        """更新表格数据"""
        if self.engine.upper() == 'POSTGIS':
            page_data = self.read_geodata()
        elif self.engine.upper() == 'FILE':
            start_row = (self.current_page - 1) * self.rows_per_page
            end_row = start_row + self.rows_per_page
            page_data = self.display_geodata.iloc[start_row:end_row]
        model = PandasModel(page_data)
        self.table_view.setModel(model)
        self.h_layout.removeItem(self.h_layout_spaceritem_left)
        self.h_layout.removeItem(self.h_layout_spaceritem_right)
        for button in self.page_buttons:
            button.deleteLater()
        # 更新按钮的状态
        self.add_page_button()
        self.prev_button.setEnabled(self.current_page > 1)
        self.next_button.setEnabled(self.current_page < self.page_num)
        self.table_view.selectionModel().selectionChanged.connect(self.on_selection_changed)

    def previous_page(self):
        """上一页"""
        if self.current_page > 0:
            self.current_page -= 1
            self.update_table()

    def next_page(self):
        current_focus = self.focusWidget()
        """下一页"""
        if self.current_page < self.page_num:
            self.current_page += 1
            self.update_table()
        # 如果按钮有焦点，手动将焦点设置到其他控件上
        if current_focus == self.next_button:
            self.prev_button.setFocus()
        # 保持焦点在原控件上（如果焦点不在按钮上）
        elif current_focus:
            current_focus.setFocus()

    def on_selection_changed(self):
        """当用户选择行时，获取该行的数据行数"""
        selected_indexes = self.table_view.selectionModel().selectedIndexes()
        if selected_indexes:
            # 获取第一个选中单元格的行号
            if self.engine.upper() == 'POSTGIS':
                actual_row = selected_indexes[0].row()
            elif self.engine.upper() == 'FILE':
                row = selected_indexes[0].row()
                actual_row = (self.current_page - 1) * self.rows_per_page + row
            # 将选中的行号与实际的行号进行映射（考虑分页）
            row = self.geodata.iloc[actual_row]  # 获取行
            geometry = row[self.geodata.geometry.name]
            data = geopandas.GeoDataFrame({'geometry': [geometry]})
            self.geodata_preview_canvas.update_plot(data=data)
            self.selected_data = geometry

    def get_page_num_from_postgis(self):
        cursor = self.postgis_con.cursor()
        cursor.execute(f'select ceil(cast(count(1) AS float)/{self.rows_per_page}) from "{self.table_name}"')
        self.page_num = int(cursor.fetchone()[0])

    def read_geodata(self):
        if self.engine.upper() == 'FILE':
            self.geodata = geopandas.read_file(filename=self.filename, encoding='utf-8')
            # 获取几何列名称
        elif self.engine.upper() == 'POSTGIS':
            self.geodata = geopandas.read_postgis(f'select * from "{self.table_name}" limit {self.rows_per_page} '
                                                  f'offset {(self.current_page-1)*self.rows_per_page}', self.postgis_con, geom_col=self.geom_col)
        self.display_geodata = self.geodata.copy()
        self.display_geodata[self.geodata.geometry.name] = self.geodata[self.geodata.geometry.name].apply(lambda x: '<geometry>')
        return self.display_geodata

    def closeEvent(self, event):
        self.closed.emit()
        event.accept()

    def set_tableview_stylesheet(self, tableview):
        if self.is_transparent_window:
            color = 'white'
        else:
            color = 'black'
        tableview.setStyleSheet(f"""
                QHeaderView {{
                    color: {color};  /* 表头字体颜色 */
                    font-weight: bold;  /* 设置表头字体加粗 */
                }}
                QTableView::verticalHeader {{
                    color: {color};  /* 垂直表头字体颜色 */
                    font-weight: bold;  /* 设置垂直表头字体加粗 */
                }}
                QTableView {{
                    font-size: 10pt;  /* 设置字体大小 */
                    color: {color};  /* 设置字体颜色 */
                }}
                QTableView::item {{
                    color: {color};  /* 设置单元格中的文本颜色 */
                }}
                QTableView::item:selected {{
                    background-color: lightblue;  /* 设置选中项的背景颜色 */
                    color: black;  /* 设置选中项的字体颜色 */
                }}
                QTableView::item:hover {{
                    background-color: lightblue;  /* 设置鼠标悬停时的背景颜色 */
                }}
                QScrollBar:vertical {{
                    border: none; /* 去掉边框 */
                    background: rgb(255,255,255); /* 滚动条背景颜色 */
                    width: 10px; /* 滚动条宽度 */
                    margin: 0px 0 0px 0; /* 滚动条上下间距 */
                }}
                QScrollBar::handle:vertical {{
                    background: rgb(219,219,219); /* 滚动条滑块颜色 */
                    min-height: 20px; /* 滚动条滑块最小高度 */
                    border-radius: 3px; /* 滚动条滑块圆角 */
                }}
                QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                    background: none; /* 上下按钮背景 */
                    height: 0px; /* 不显示按钮 */
                }}
                QScrollBar::handle:vertical:hover {{
                    background: lightblue; /* 滚动条滑块悬停颜色 */
                }}
                 /* 水平滚动条 */
                QScrollBar:horizontal {{
                    background: rgb(219,219,219); /* 设置滚动条背景颜色 */
                    height: 10px;  /* 设置水平滚动条的高度 */
                    margin: 0px 0px 0px 0px;  /* 控制滚动条的上下间距 */
                }}
                QScrollBar::handle:horizontal {{
                    background: rgb(219,219,219);  /* 设置滚动条滑块颜色 */
                    border-radius: 5px;  /* 滚动条滑块圆角 */
                    min-width: 20px;  /* 设置滑块的最小宽度 */
                }}
                QScrollBar::handle:horizontal:hover {{
                    background: lightblue;  /* 滚动条滑块悬停时的颜色 */
                }}
                QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
                    background: none; /* 上下按钮背景 */
                    height: 0px; /* 不显示按钮 */
                }}
            """)

    @staticmethod
    def set_lineedit_stylesheet(lineedit):
        lineedit.setStyleSheet("""
                QLineEdit {
                    border: 0.5px solid rgb(219,219,219); /* 边框颜色和宽度 */
                    border-radius: 3px; /* 圆角 */
                    padding: 0.5px; /* 内边距 */
                    background-color: #f0f0f0; /* 背景颜色 */
                    color: rgb(0,0,0);
                }
                QLineEdit:focus {
                    border: 0.5px solid rgb(219,219,219); /* 聚焦时边框颜色 */
                }
                QLineEdit:disabled {
                    background-color: rgb(199,199,199); /* 禁用状态背景色 */
                    color: rgb(120,120,120); /* 禁用状态文本颜色 */
                }
        """)

    def set_label_stylesheet(self, label):
        if self.is_transparent_window:
            color = 'white'
        else:
            color = 'black'
        label.setStyleSheet(f"""
                color: {color}; 
                QLabel {{
                   font-size: 18px;
                   padding: 20px;
                }}
        """)

    @staticmethod
    def set_pushbutton_stylesheet(pushbutton):
        if pushbutton.text() == '下一页' or pushbutton.text() == '上一页':
            pushbutton.setStyleSheet("""
                        QPushButton {
                            background-color: rgb(243,243,243); /* 正常状态背景色 */
                            color: black; /* 正常状态文本颜色 */
                            border: 0.5px solid rgb(219,219,219); /* 边框颜色 */
                            border-radius: 3px; /* 圆角 */
                            padding: 0px; /* 内边距 */
                        }
                        QPushButton:hover {
                            background-color: lightblue; /* 悬停时背景色 */
                        }
                        QPushButton:pressed {
                            background-color: lightblue; /* 按下时背景色 */
                        }
                        QPushButton:disabled {
                            background-color: rgb(199,199,199); /* 禁用状态背景色 */
                            color: rgb(120,120,120); /* 禁用状态文本颜色 */
                        }
                """)
        else:
            pushbutton.setStyleSheet("""
                        QPushButton {
                            background-color: rgb(243,243,243); /* 正常状态背景色 */
                            color: black; /* 正常状态文本颜色 */
                            border: 0.5px solid rgb(219,219,219); /* 边框颜色 */
                            border-radius: 3px; /* 圆角 */
                            padding: 0px; /* 内边距 */
                        }
                        QPushButton:hover {
                            background-color: lightblue; /* 悬停时背景色 */
                        }
                        QPushButton:pressed {
                            background-color: lightblue; /* 按下时背景色 */
                        }
                        QPushButton:disabled {
                            background-color: lightblue; /* 禁用状态背景色 */
                            color: rgb(120,120,120); /* 禁用状态文本颜色 */
                        }
                """)


class PandasModel(QAbstractTableModel):
    def __init__(self, data_frame):
        super().__init__()
        self._data_frame = data_frame

    def rowCount(self, parent=QModelIndex()):
        return len(self._data_frame)

    def columnCount(self, parent=QModelIndex()):
        return len(self._data_frame.columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.DisplayRole:
            return str(self._data_frame.iloc[index.row(), index.column()])
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return self._data_frame.columns[section]
            else:
                return str(section + 1)  # 显示行号
        return None


class PlotCanvas(FigureCanvas):
    def __init__(self, parent=None, dpi=100):
        super().__init__(Figure(dpi=dpi))
        self.setParent(parent)
        # Create the figure and axes
        self.canvas = FigureCanvas(self.figure)
        self.axes = self.figure.add_subplot(111)
        self.axes.set_axis_off()
        self.bgcolor = (243 / 255, 243 / 255, 243 / 255)
        self.figure.set_facecolor(self.bgcolor)
        self.axes.set_facecolor(self.bgcolor)

    def update_plot(self, data):
        self.axes.clear()
        self.axes.set_axis_off()
        self.axes.set_facecolor(self.bgcolor)
        self.figure.set_facecolor(self.bgcolor)
        data.plot(ax=self.axes, edgecolor='#FF0000', facecolor='#FF0000', alpha=0.5)
        # self.axes.set_title('矢量数据预览')
        self.draw()
