from __future__ import annotations
import os
os.environ["QT_LOGGING_RULES"] = "qt.multimedia.playbackengine.codec=false"
import sys
import datetime
import subprocess
import webbrowser
import traceback
import concurrent.futures
from pathlib import Path
import shutil
import time
import random
import threading
import queue
import sqlite3
from PySide6.QtCore import (
    Qt, QTimer, QSize, QSettings, QSortFilterProxyModel, QAbstractTableModel,
    QModelIndex, QMimeData, Signal, QObject, QThread, QEvent, QPoint, QUrl,
    QThreadPool, QRunnable, QRect, QItemSelection, QItemSelectionModel
)
from PySide6.QtGui import (
    QAction, QKeySequence, QIcon, QDrag, QPixmap, QPainter, QConicalGradient, QColor, QPen, QResizeEvent, QCursor, QMouseEvent
)
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTableView, QStyle, QHeaderView, QAbstractItemView,
    QListWidget, QListWidgetItem, QMenu, QFileDialog, QLabel, QPushButton,
    QLineEdit, QSlider, QMessageBox, QProgressDialog, QSizePolicy, QGridLayout,
    QDialog, QCheckBox, QComboBox, QSpacerItem, QScrollBar, QScrollArea,
    QStyledItemDelegate, QTextEdit, QRubberBand
)
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput
from PySide6.QtMultimediaWidgets import QVideoWidget
from PySide6.QtNetwork import QLocalServer, QLocalSocket
import librosa

ERROR_LOG_FILE = "error.log"
APP_NAME = "Karaoke Player"
SETTINGS_FILE = "config.ini"
HISTORY_LOG_FILE = "history.log"
SUPPORTED_FILE_EXTENSIONS = [".mp4", ".mkv", ".avi", ".cdg"]

IDLES_FOLDER = "Idles"

def top_level_get_duration(path_str):
    import os, subprocess
    if not os.path.exists(path_str):
        return 0
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path_str
    ]
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace', startupinfo=startupinfo)
    if result.returncode == 0:
        try:
            length = float(result.stdout.strip())
            return int(length * 1000)
        except Exception:
            return 0
    return 0
def createThumbnail(path_str):
    from pathlib import Path
    import subprocess, os
    if not os.path.exists(path_str):
        return
    ext = Path(path_str).suffix.casefold()
    Path("thumbs").mkdir(exist_ok=True)
    thumb_path = os.path.join("thumbs", os.path.basename(path_str) + ".jpg")
    if os.path.exists(thumb_path):
        return
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    if ext in (".mp4", ".mkv", ".avi"):
        cmd = [
            "ffmpeg", "-y", "-ss", "35", "-i", path_str,
            "-frames:v", "1", "-vf", "scale=87:49",
            thumb_path
        ]
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       text=True, encoding='utf-8', errors='replace',
                       startupinfo=startupinfo)
    elif ext == ".cdg":
        cmd = [
            "ffmpeg", "-y", "-fflags", "+genpts", "-f", "cdg", "-i", path_str,
            "-ss", "35", "-frames:v", "1",
            "-vf", "format=rgb24,scale=65:49:force_original_aspect_ratio=decrease,pad=87:49:11:0:#181818",
            thumb_path
        ]
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       text=True, encoding='utf-8', errors='replace',
                       startupinfo=startupinfo)
    else:
        return

def worker_func_for_scan(f):
    fn = f.name
    extension = f.suffix.casefold()
    artist, title = parse_filename_for_artist_song(fn)
    dur = top_level_get_duration(str(f))
    return (fn, extension, artist, title, dur)

def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)
def cleanThumbs():
    """Delete thumbnail files for songs that are no longer in the database."""
    import sqlite3
    from pathlib import Path
    valid = set()
    conn = sqlite3.connect("library.db")
    c = conn.cursor()
    c.execute("SELECT filename FROM songs")
    for row in c.fetchall():
        valid.add(row[0])
    conn.close()
    thumbs_dir = Path("thumbs")
    if thumbs_dir.exists():
        for thumb in thumbs_dir.iterdir():
            if thumb.is_file() and thumb.suffix.lower() == ".jpg":
                base = os.path.splitext(thumb.name)[0]
                if base not in valid:
                    try:
                        thumb.unlink()
                    except Exception as e:
                        log_error(f"Failed to delete thumbnail {thumb}: {e}")

def log_error(message: str):
    try:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")
    except Exception:
        pass

def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    try:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Uncaught exception:\n")
            traceback_str = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            f.write(traceback_str + "\n")
    except Exception:
        pass

sys.excepthook = handle_uncaught_exception

def ms_to_mmss(milliseconds):
    if milliseconds <= 0:
        return "--:--"
    sec = milliseconds // 1000
    mm = sec // 60
    ss = sec % 60
    return f"{mm}:{ss:02}"

def parse_filename_for_artist_song(filename: str):
    base = os.path.splitext(os.path.basename(filename))[0]
    base = base.replace("(AutoRealKaraoke)", "").replace("_", " ")
    if " - " in base:
        artist, song = base.split(" - ", 1)
        return artist.strip(), song.strip()
    else:
        return "Unknown Artist", base.strip()

def check_single_instance(server_name="KaraokePlayerInstance"):
    socket = QLocalSocket()
    socket.connectToServer(server_name)
    if socket.waitForConnected(100):
        return None  
    server = QLocalServer()
    server.listen(server_name)
    return server

class SearchLineEdit(QLineEdit):
    enterPressed = Signal()
    def keyPressEvent(self, event):
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            self.enterPressed.emit()
        super().keyPressEvent(event)

class CustomVideoWidget(QVideoWidget):
    doubleClicked = Signal()
    def mouseDoubleClickEvent(self, event):
        super().mouseDoubleClickEvent(event)
        self.doubleClicked.emit()

class ClickableLabel(QLabel):
    clicked = Signal()
    def mousePressEvent(self, event):
        self.clicked.emit()
        super().mousePressEvent(event)

class TempSettingsDialog(QDialog):
    def __init__(self, parent: KaraokePlayer):
        super().__init__(parent)
        self.main_app = parent
        self.setWindowTitle("Settings")
        self.setModal(True)
        self.resize(470, 280)
        main_layout = QVBoxLayout(self)
        top_layout = QHBoxLayout()
        logo_label = QLabel(self)
        logo_label.setAlignment(Qt.AlignCenter)
        logo_pix = QPixmap(resource_path("karaokeplayerlogo259.png"))
        scaled_pix = logo_pix.scaled(logo_pix.width() // 1.8, logo_pix.height() // 1.8, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        logo_label.setPixmap(scaled_pix)
        logo_label.setFixedSize(scaled_pix.size())
        top_layout.addWidget(logo_label)
        controls_layout = QVBoxLayout()
        folder_row = QHBoxLayout()
        self.size_label = QLabel("")
        self.updateFolderSizeLabel()
        folder_row.addWidget(self.size_label)
        folder_row.addStretch()
        self.delete_button = QPushButton("Delete Renders")
        self.delete_button.clicked.connect(self.deleteRenders)
        folder_row.addWidget(self.delete_button)
        controls_layout.addLayout(folder_row)
        self.auto_delete_checkbox = QCheckBox("Auto-delete on Player open")
        auto_del = self.main_app.settings.value("autoDeleteTemp", True, type=bool)
        self.auto_delete_checkbox.setChecked(auto_del)
        self.auto_delete_checkbox.toggled.connect(self.updateAutoDeleteSetting)
        controls_layout.addWidget(self.auto_delete_checkbox)
        controls_layout.addSpacing(10)
        self.search_enter_checkbox = QCheckBox('Search requires "Enter" key press.\n(Requires restart.)')
        search_enter = self.main_app.settings.value("searchRequiresEnter", True, type=bool)
        self.search_enter_checkbox.setChecked(search_enter)
        self.search_enter_checkbox.toggled.connect(lambda state: self.main_app.settings.setValue("searchRequiresEnter", state))
        controls_layout.addWidget(self.search_enter_checkbox)
        controls_layout.addSpacing(10)
        self.idle_dropdown_label = QLabel("Idle loop first:")
        self.idle_dropdown = QComboBox()
        self.idle_dropdown.setToolTip("Pick which .mp4 from the Idles folder is used at startup.")
        self.idles_list = []
        idles_folder = Path("Idles")
        idles_folder.mkdir(exist_ok=True)
        for f in idles_folder.glob("*.mp4"):
            self.idles_list.append(f.name)
        self.idles_list.sort()
        for f in self.idles_list:
            self.idle_dropdown.addItem(f)
        default_idle = self.main_app.settings.value("defaultIdle", "wire.mp4")
        if default_idle in self.idles_list:
            self.idle_dropdown.setCurrentText(default_idle)
        self.idle_dropdown.currentIndexChanged.connect(self.onIdleDropdownChanged)
        controls_layout.addWidget(self.idle_dropdown_label)
        controls_layout.addWidget(self.idle_dropdown)
        self.idle_change_label = QLabel("Idle loop changes every (sec):")
        self.idle_change_dropdown = QComboBox()
        self.idle_change_dropdown.setToolTip("Pick how often to auto-change to a random .mp4 from the Idles folder. 'None' means no change.")
        self.idle_change_dropdown.addItem("None")
        intervals = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        for it in intervals:
            self.idle_change_dropdown.addItem(str(it))
        stored_interval = self.main_app.settings.value("idleChangeInterval", 30, type=int)
        if stored_interval <= 0:
            self.idle_change_dropdown.setCurrentText("None")
        elif stored_interval in intervals:
            self.idle_change_dropdown.setCurrentText(str(stored_interval))
        else:
            self.idle_change_dropdown.setCurrentText("30")
        self.idle_change_dropdown.currentIndexChanged.connect(self.onIdleChangeIntervalChanged)
        controls_layout.addWidget(self.idle_change_label)
        controls_layout.addWidget(self.idle_change_dropdown)
        top_layout.addLayout(controls_layout)
        main_layout.addLayout(top_layout)
        main_layout.addStretch()
        version_label = QLabel("Karaoke Player v1.1 (build 250310) by profzelonka", self)
        version_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(version_label)

    def onIdleDropdownChanged(self):
        new_idle = self.idle_dropdown.currentText()
        self.main_app.settings.setValue("defaultIdle", new_idle)

    def onIdleChangeIntervalChanged(self):
        val_str = self.idle_change_dropdown.currentText()
        if val_str == "None":
            self.main_app.settings.setValue("idleChangeInterval", 0)
        else:
            val_int = int(val_str)
            self.main_app.settings.setValue("idleChangeInterval", val_int)

    def updateFolderSizeLabel(self):
        folder = self.main_app.temp_folder
        total_bytes = 0
        for dirpath, _, filenames in os.walk(folder):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.isfile(fp):
                    total_bytes += os.path.getsize(fp)
        if total_bytes < 1024:
            size_str = f"{total_bytes} Bytes"
        elif total_bytes < 1024*1024:
            size_str = f"{total_bytes/1024:.1f} KB"
        else:
            size_str = f"{total_bytes/(1024*1024):.1f} MB"
        self.size_label.setText(f"Temp folder size: {size_str}")

    def deleteRenders(self):
        folder = self.main_app.temp_folder
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Failed to delete {file_path}.\n{e}")
        self.updateFolderSizeLabel()

    def updateAutoDeleteSetting(self, checked: bool):
        self.main_app.settings.setValue("autoDeleteTemp", checked)
        self.main_app.settings.sync()

class SongItem:
    def __init__(self, file_path: str, file_type: str, artist: str, title: str, duration_ms: int):
        self.file_path = file_path
        self.file_type = file_type
        self.artist = artist
        self.title = title
        self.duration_ms = duration_ms
        self.duration_str = ms_to_mmss(duration_ms)
        self.key_change = 0
        self.tempo_change = 0
        self.shifted_audio_path = None
        self.tempo_shifted_audio_path = None
        self.key_tempo_shifted_audio_path = None
        self.silence_detection_done = False
        self.intro_start_ms = 0
        self.outro_start_ms = 9999999
        self.history_dt = ""
        self.lib_name = None
        self.is_rendering = False
        self.render_intent = None 
        
    def __repr__(self):
        return f"SongItem({self.file_path})"

    @property
    def audio_file_path(self):
        if self.file_type.casefold() == ".cdg":
            base = os.path.splitext(self.file_path)[0]
            return base + ".mp3"
        return self.file_path

    def get_combined_shifted_audio_path(self, temp_folder: Path):
        parts = []
        if self.key_change != 0:
            parts.append(f"Key{self.key_change}")
        if self.tempo_change != 0:
            parts.append(f"Tempo{self.tempo_change * 5}")
        if not parts:
            return None
        combined_suffix = "_".join(parts)
        base = Path(self.file_path).stem
        if self.file_type.casefold() in (".mp4", ".mkv", ".avi"):
            shifted_audio_filename = f"{base}_{combined_suffix}{self.file_type}"
        else:
            shifted_audio_filename = f"{base}_{combined_suffix}.flac"
        return str(temp_folder / shifted_audio_filename)

class CDGOverlayWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        self.setAttribute(Qt.WA_TranslucentBackground, True)
        self.setStyleSheet("background: transparent;")
    def setOverlayEnabled(self, enabled):
        pass
    def paintEvent(self, event):
        pass

class CategoryRow(QWidget):
    plusClicked = Signal()
    rowClicked = Signal() 

    def __init__(self, 
                 text, 
                 has_plus=False, 
                 collapsible=False, 
                 toggle_callback=None, 
                 parent=None, 
                 fixed_height=None):
        super().__init__(parent)
        self.setObjectName("CategoryRow")
        self.text = text
        self.has_plus = has_plus
        self.collapsible = collapsible
        self.toggle_callback = toggle_callback
        self.expanded = False

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(4)

        if self.collapsible:
            self.arrow_button = QPushButton("▶", self)
            self.arrow_button.setFixedSize(20, 20)
            self.arrow_button.setFlat(True)
            self.arrow_button.setStyleSheet("QPushButton { background-color: #2a2a2a; color: #FFFFFF; border: none; }")
            self.arrow_button.clicked.connect(self.toggle)
            layout.addWidget(self.arrow_button)

        self.label = QLabel(text, self)
        fnt = self.label.font()
        fnt.setBold(True)
        fnt.setPointSize(12)
        self.label.setFont(fnt)
        self.label.setStyleSheet("QLabel { color: #FFFFFF; }")
        layout.addWidget(self.label, 1)

        if self.has_plus:
            self.plus_btn = QPushButton("+", self)
            self.plus_btn.setFixedSize(20, 20)
            self.plus_btn.setStyleSheet(
                "QPushButton { background-color: #2A2A2A; color: #FFFFFF; border: none; }"
                "QPushButton:hover { background-color: #333333; }"
            )
            self.plus_btn.clicked.connect(self.plusClicked.emit)
            layout.addWidget(self.plus_btn, 0)
        else:
            self.plus_btn = None

        if fixed_height:
            self.setFixedHeight(fixed_height)

        self.setStyleSheet("QWidget#CategoryRow { background-color: #202020; }")

    def toggle(self):
        self.expanded = not self.expanded
        if self.collapsible:
            self.arrow_button.setText("▼" if self.expanded else "▶")
        if self.toggle_callback:
            self.toggle_callback()

    def mousePressEvent(self, event):
        if (not self.collapsible) and (not self.has_plus):
            if event.button() == Qt.LeftButton:
                self.rowClicked.emit()
        super().mousePressEvent(event)

    def setExpanded(self, expanded: bool):
        self.expanded = expanded
        if self.collapsible and self.arrow_button:
            self.arrow_button.setText("▼" if expanded else "▶")

    def setText(self, text):
        self.text = text
        self.label.setText(text)

class SongsTableModel(QAbstractTableModel):
    def __init__(self, songs=None, show_key_tempo=False, view_mode="default"):
        super().__init__()
        self._songs = songs if songs else []
        self.show_key_tempo = show_key_tempo
        self.editable_order = False
        self.history_mode = False
        self.list_mode = False

    def rowCount(self, parent=None):
        return len(self._songs)

    def columnCount(self, parent=None):
        if self.history_mode:
            return 7 if self.show_key_tempo else 5
        elif self.list_mode:
            return 7 if self.show_key_tempo else 5
        else:
            return 6 if self.show_key_tempo else 4

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.TextAlignmentRole:
            return Qt.AlignLeft | Qt.AlignVCenter
        if role in (Qt.DisplayRole, Qt.EditRole):
            song = self._songs[index.row()]
            col = index.column()
            if self.history_mode:
                if self.show_key_tempo:
                    if col == 0:
                        return song.title
                    elif col == 1:
                        return song.artist
                    elif col == 2:
                        return song.duration_str
                    elif col == 3:
                        return song.file_type.lstrip(".")
                    elif col == 4:
                        return f"{'+' if song.key_change>0 else ''}{song.key_change}" if song.key_change != 0 else ""
                    elif col == 5:
                        tval = song.tempo_change * 5
                        return f"{'+' if tval>0 else ''}{tval}%" if tval != 0 else ""
                    elif col == 6:
                        return song.history_dt
                else:
                    if col == 0:
                        return song.title
                    elif col == 1:
                        return song.artist
                    elif col == 2:
                        return song.duration_str
                    elif col == 3:
                        return song.file_type.lstrip(".")
                    elif col == 4:
                        return song.history_dt
            elif self.list_mode:
                if self.show_key_tempo:
                    if col == 0:
                        return song.title
                    elif col == 1:
                        return song.artist
                    elif col == 2:
                        return song.duration_str
                    elif col == 3:
                        return song.file_type.lstrip(".")
                    elif col == 4:
                        return f"{'+' if song.key_change>0 else ''}{song.key_change}" if song.key_change != 0 else ""
                    elif col == 5:
                        tval = song.tempo_change * 5
                        return f"{'+' if tval>0 else ''}{tval}%" if tval != 0 else ""
                    elif col == 6:
                        return song.lib_name if song.lib_name else ""
                else:
                    if col == 0:
                        return song.title
                    elif col == 1:
                        return song.artist
                    elif col == 2:
                        return song.duration_str
                    elif col == 3:
                        return song.file_type.lstrip(".")
                    elif col == 4:
                        return song.lib_name if song.lib_name else ""
            else:
                if self.show_key_tempo:
                    if col == 0:
                        return song.title
                    elif col == 1:
                        return song.artist
                    elif col == 2:
                        return song.duration_str
                    elif col == 3:
                        return song.file_type.lstrip(".")
                    elif col == 4:
                        return f"{'+' if song.key_change>0 else ''}{song.key_change}" if song.key_change != 0 else ""
                    elif col == 5:
                        tval = song.tempo_change * 5
                        return f"{'+' if tval>0 else ''}{tval}%"
                else:
                    if col == 0:
                        return song.title
                    elif col == 1:
                        return song.artist
                    elif col == 2:
                        return song.duration_str
                    elif col == 3:
                        return song.file_type.lstrip(".")
        return None


    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal:
            if role == Qt.TextAlignmentRole:
                return Qt.AlignLeft | Qt.AlignVCenter
            if role == Qt.DisplayRole:
                if self.history_mode:
                    if self.show_key_tempo:
                        headers = ["Song", "Artist", "Duration", "File Type", "Key", "Tempo", "When"]
                    else:
                        headers = ["Song", "Artist", "Duration", "File Type", "When"]
                elif self.list_mode:
                    if self.show_key_tempo:
                        headers = ["Song", "Artist", "Duration", "File Type", "Key", "Tempo", "Library"]
                    else:
                        headers = ["Song", "Artist", "Duration", "File Type", "Library"]
                else:
                    if self.show_key_tempo:
                        headers = ["Song", "Artist", "Duration", "File Type", "Key", "Tempo"]
                    else:
                        headers = ["Song", "Artist", "Duration", "File Type"]
                if section < len(headers):
                    return headers[section]
                return ""
        if orientation == Qt.Vertical:
            if role == Qt.DisplayRole:
                return str(section + 1)
            elif role == Qt.TextAlignmentRole:
                return Qt.AlignCenter
        return super().headerData(section, orientation, role)

    def getSongItem(self, row):
        if 0 <= row < len(self._songs):
            return self._songs[row]
        return None

    def setSongs(self, songs):
        self.beginResetModel()
        self._songs = songs
        self.endResetModel()

    def songs(self):
        return self._songs

    def flags(self, index):
        default_flags = super().flags(index)
        if self.editable_order:
            return default_flags | Qt.ItemIsDragEnabled | Qt.ItemIsDropEnabled | Qt.ItemIsEditable
        return default_flags

    def supportedDropActions(self):
        return Qt.MoveAction if self.editable_order else super().supportedDropActions()

    def supportedDragActions(self):
        return Qt.MoveAction if self.editable_order else super().supportedDragActions()

    def moveRows(self, sourceParent, sourceRow, count, destinationParent, destinationChild):
        if sourceRow < 0 or sourceRow + count > len(self._songs):
            return False
        self.beginMoveRows(sourceParent, sourceRow, sourceRow + count - 1, destinationParent, destinationChild)
        moving = self._songs[sourceRow:sourceRow+count]
        del self._songs[sourceRow:sourceRow+count]
        if destinationChild > sourceRow:
            destinationChild -= count
        for i, item in enumerate(moving):
            self._songs.insert(destinationChild + i, item)
        self.endMoveRows()
        return True

    def dropMimeData(self, data, action, row, column, parent):
        if self.editable_order and data.hasText():
            try:
                rows = sorted([int(x) for x in data.text().split(",") if x])
            except Exception:
                return False
            if not rows:
                return False
            if row == -1:
                row = self.rowCount()
            consecutive = all(rows[i] + 1 == rows[i+1] for i in range(len(rows)-1))
            if consecutive:
                return self.moveRows(QModelIndex(), rows[0], len(rows), QModelIndex(), row)
            else:
                for r in sorted(rows, reverse=True):
                    if r < row:
                        if not self.moveRows(QModelIndex(), r, 1, QModelIndex(), row-1):
                            return False
                        row -= 1
                    else:
                        if not self.moveRows(QModelIndex(), r, 1, QModelIndex(), row):
                            return False
                return True
        return False

    def sort(self, column, order=Qt.AscendingOrder):
        reverse = (order == Qt.DescendingOrder)
        if column == 0:
            key_func = lambda s: s.title.casefold()
        elif column == 1:
            key_func = lambda s: s.artist.casefold()
        elif column == 2:
            key_func = lambda s: s.duration_ms
        elif column == 3:
            key_func = lambda s: s.file_type.casefold()
        elif self.show_key_tempo:
            if column == 4:
                key_func = lambda s: s.key_change
            elif column == 5:
                key_func = lambda s: s.tempo_change
            else:
                key_func = lambda s: datetime.datetime.strptime(s.history_dt, "%H:%M:%S %m-%d-%Y") if s.history_dt else datetime.datetime.min
        else:
            key_func = lambda s: datetime.datetime.strptime(s.history_dt, "%H:%M:%S %m-%d-%Y") if s.history_dt else datetime.datetime.min
        self.layoutAboutToBeChanged.emit()
        self._songs.sort(key=key_func, reverse=reverse)
        self.layoutChanged.emit()

class TwoFieldFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.songFilter = ""
        self.artistFilter = ""

    def setSongFilter(self, text):
        self.songFilter = text.casefold()

    def setArtistFilter(self, text):
        self.artistFilter = text.casefold()

    def filterAcceptsRow(self, source_row, source_parent):
        from unicodedata import normalize
        index_song = self.sourceModel().index(source_row, 0, source_parent)
        index_artist = self.sourceModel().index(source_row, 1, source_parent)
        song = self.sourceModel().data(index_song, Qt.DisplayRole) or ""
        artist = self.sourceModel().data(index_artist, Qt.DisplayRole) or ""
        song_cf = normalize("NFKD", song).casefold()
        artist_cf = normalize("NFKD", artist).casefold()
        if self.songFilter and self.songFilter not in song_cf:
            return False
        if self.artistFilter and self.artistFilter not in artist_cf:
            return False
        return True

    def lessThan(self, left, right):
        source = self.sourceModel()
        sort_col = self.sortColumn()
        left_data = source.data(source.index(left.row(), sort_col))
        right_data = source.data(source.index(right.row(), sort_col))
        if sort_col == 1 and left_data == right_data:
            left_title = source.data(source.index(left.row(), 0))
            right_title = source.data(source.index(right.row(), 0))
            return left_title < right_title
        return left_data < right_data

class CombinedShiftWorker(QObject):
    progress = Signal(int)
    finished = Signal(bool, str)
    def __init__(self, song_item: SongItem, key_factor: float, tempo_factor: float, temp_folder: Path):
        super().__init__()
        self.song_item = song_item
        self.key_factor = key_factor
        self.tempo_factor = tempo_factor
        self.temp_folder = temp_folder
        self._is_cancelled = False

    def run(self):
        try:
            import subprocess
            from pathlib import Path
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            original_file = Path(self.song_item.audio_file_path)
            if not original_file.exists():
                original_file = Path(self.song_item.file_path)
            if not original_file.exists():
                self.finished.emit(False, "Original file does not exist.")
                return
            if self.song_item.key_change == 0 and self.song_item.tempo_change == 0:
                self.finished.emit(True, "")
                return

            pitch_change_needed = (self.song_item.key_change != 0)
            tempo_change_needed = (self.song_item.tempo_change != 0)
            pitch_factor = self.key_factor
            tempo_factor = self.tempo_factor
            combined_suffix = []
            if pitch_change_needed:
                combined_suffix.append(f"Key{self.song_item.key_change}")
            if tempo_change_needed:
                combined_suffix.append(f"Tempo{self.song_item.tempo_change * 5}")
            combo_str = "_".join(combined_suffix)
            original_ext = self.song_item.file_type.casefold()
            if original_ext in (".mp4", ".mkv", ".avi"):
                shifted_out_filename = f"{Path(self.song_item.file_path).stem}_{combo_str}{original_ext}"
            else:
                shifted_out_filename = f"{Path(self.song_item.file_path).stem}_{combo_str}.flac"
            shifted_out_path = self.temp_folder / shifted_out_filename
            if pitch_change_needed and tempo_change_needed:
                self.song_item.key_tempo_shifted_audio_path = str(shifted_out_path)
            elif pitch_change_needed and not tempo_change_needed:
                self.song_item.shifted_audio_path = str(shifted_out_path)
            elif tempo_change_needed and not pitch_change_needed:
                self.song_item.tempo_shifted_audio_path = str(shifted_out_path)
            if shifted_out_path.exists():
                self.finished.emit(True, "")
                return

            cmd_duration = [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(original_file)
            ]
            result = subprocess.run(
                cmd_duration,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True, encoding='utf-8', errors='replace',
                startupinfo=startupinfo
            )
            if result.returncode != 0:
                self.finished.emit(False, "Failed to get media duration. Ensure ffmpeg is installed.")
                return
            try:
                total_duration = float(result.stdout.strip())
            except ValueError:
                self.finished.emit(False, "Invalid duration from ffprobe.")
                return
            af_filters = []
            if pitch_change_needed:
                af_filters.append(f"rubberband=pitch={pitch_factor}")
            if tempo_change_needed:
                af_filters.append(f"atempo={tempo_factor}")
            audio_chain = ",".join(af_filters)
            vf_filters = []
            if tempo_change_needed:
                vf_filters.append(f"setpts=PTS/{tempo_factor}")
            video_chain = ",".join(vf_filters)
            if original_ext not in (".mp4", ".mkv", ".avi"):
                if not audio_chain:
                    self.finished.emit(True, "")
                    return
                cmd = [
                    "ffmpeg", "-y",
                    "-i", str(original_file),
                    "-filter:a", audio_chain,
                    "-vn",
                    "-c:a", "flac",
                    str(shifted_out_path)
                ]
            else:
                pitch_or_tempo_applied = (audio_chain or video_chain)
                if not pitch_or_tempo_applied:
                    self.finished.emit(True, "")
                    return
                if audio_chain and video_chain:
                    filter_complex = f"[0:a]{audio_chain}[a];[0:v]{video_chain}[v]"
                    cmd = [
                        "ffmpeg", "-y",
                        "-i", str(original_file),
                        "-filter_complex", filter_complex,
                        "-map", "[v]",
                        "-map", "[a]"
                    ]
                    if tempo_change_needed:
                        cmd += ["-c:v", "libx264"]
                    else:
                        cmd += ["-c:v", "copy"]
                    cmd += [
                        "-c:a", "flac",
                        str(shifted_out_path)
                    ]
                elif audio_chain:
                    cmd = [
                        "ffmpeg", "-y",
                        "-i", str(original_file),
                        "-filter:a", audio_chain,
                        "-map", "0:v",
                        "-map", "0:a",
                        "-c:v", "copy",
                        "-c:a", "flac",
                        str(shifted_out_path)
                    ]
                elif video_chain:
                    cmd = [
                        "ffmpeg", "-y",
                        "-i", str(original_file),
                        "-filter_complex", f"[0:v]{video_chain}[v]",
                        "-map", "[v]",
                        "-map", "0:a",
                        "-c:a", "copy",
                        "-c:v", "libx264",
                        str(shifted_out_path)
                    ]
                else:
                    self.finished.emit(True, "")
                    return

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True, encoding='utf-8', errors='replace',
                startupinfo=startupinfo
            )
            start_time = time.time()
            while True:
                if self._is_cancelled:
                    process.terminate()
                    self.finished.emit(False, "Combined key/tempo change cancelled.")
                    return
                line = process.stderr.readline()
                if not line:
                    break
                if "time=" in line:
                    try:
                        time_str = line.strip().split("time=")[1].split(" ")[0]
                        h, m, s = time_str.split(':')
                        elapsed_media = float(h)*3600 + float(m)*60 + float(s)
                        conversion_progress = elapsed_media / total_duration
                        real_elapsed = time.time() - start_time
                        if conversion_progress > 0:
                            estimated_total = real_elapsed / conversion_progress
                        else:
                            estimated_total = real_elapsed
                        friendly_remaining = int(estimated_total - real_elapsed)
                        if friendly_remaining < 0:
                            friendly_remaining = 0
                        self.progress.emit(friendly_remaining)
                    except Exception:
                        pass
            process.wait()
            if process.returncode != 0:
                self.finished.emit(False, "FFmpeg failed during combined key/tempo change.")
                return
            self.finished.emit(True, "")
        except Exception as e:
            self.finished.emit(False, f"An unexpected error occurred: {str(e)}")

    def cancel(self):
        self._is_cancelled = True

class DragDropListWidget(QListWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setDragEnabled(False)
        self.setAcceptDrops(True)
        self.setSelectionMode(QAbstractItemView.SingleSelection)
        self.parent_ref = parent

    def dragEnterEvent(self, e):
        if e.mimeData().hasText():
            e.acceptProposedAction()
        else:
            super().dragEnterEvent(e)

    def dragMoveEvent(self, e):
        if e.mimeData().hasText():
            item = self.itemAt(e.position().toPoint())
            if item:
                role = item.data(Qt.UserRole)
                if role in {"QueueCategory", "ListSub"}:
                    e.acceptProposedAction()
                    return
        super().dragMoveEvent(e)

    def dropEvent(self, e):
        if e.mimeData().hasUrls():
            from pathlib import Path
            from PySide6.QtCore import QUrl
            e.setDropAction(Qt.CopyAction)
            e.accept()
            for url in e.mimeData().urls():
                local_path = url.toLocalFile()
                if local_path:
                    p = Path(local_path)
                    if p.suffix.lower() in SUPPORTED_FILE_EXTENSIONS and p.exists():
                        artist, title = parse_filename_for_artist_song(p.name)
                        dur = top_level_get_duration(str(p))
                        song_item = SongItem(str(p), p.suffix.lower(), artist, title, dur)
                        self.parent_ref.addToQueue(song_item)
            return
        if not e.mimeData().hasText():
            super().dropEvent(e)
            return
        item = self.itemAt(e.position().toPoint())
        if not item:
            super().dropEvent(e)
            return
        role = item.data(Qt.UserRole)
        if role not in ("QueueCategory", "ListSub"):
            e.ignore()
            return
        row_data = e.mimeData().data("application/x-qabstractitemmodeldatalist")
        row_numbers_str = row_data.data().decode().strip()
        if not row_numbers_str:
            super().dropEvent(e)
            return
        row_numbers = []
        for r in row_numbers_str.split(","):
            r = r.strip()
            if r:
                try:
                    row_numbers.append(int(r))
                except:
                    pass
        new_songs = []
        table_view = self.parent_ref.table_view
        current_model = table_view.model()
        for r in row_numbers:
            si = None
            if hasattr(current_model, "getSongItem"):
                si = current_model.getSongItem(r)
            elif current_model == self.parent_ref.proxy_model:
                source_index = None
                for _row in range(current_model.rowCount()):
                    if _row == r:
                        source_index = current_model.index(_row, 0)
                        break
                if source_index and source_index.isValid():
                    si = self.parent_ref.songs_model.getSongItem(source_index.row())
            if si:
                copied = SongItem(si.file_path, si.file_type, si.artist, si.title, si.duration_ms)
                copied.key_change = si.key_change
                copied.tempo_change = si.tempo_change
                copied.shifted_audio_path = si.shifted_audio_path
                copied.tempo_shifted_audio_path = si.tempo_shifted_audio_path
                copied.key_tempo_shifted_audio_path = si.key_tempo_shifted_audio_path
                copied.lib_name = si.lib_name
                new_songs.append(copied)
        if role == "QueueCategory":
            for s in new_songs:
                self.parent_ref.addToQueue(s)
            if self.parent_ref.current_view_mode == "queue":
                self.parent_ref.showQueue()
        elif role == "ListSub":
            list_name = item.text().strip()
            existing = self.parent_ref.loadListFromFile(list_name)
            for s in new_songs:
                if not any(x.file_path == s.file_path for x in existing):
                    existing.append(s)
            self.parent_ref.saveListToFile(list_name, existing)
        e.acceptProposedAction()

class DragDropTableView(QTableView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)
        self.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.setMouseTracking(True)
        self.viewport().setMouseTracking(True)
        self.parent_ref = parent

        self._mousePressPos = None
        self.hovered_row = -1
        self._drag_indexes = []

        self.rubber_band = QRubberBand(QRubberBand.Rectangle, self.viewport())
        self.drag_start_position = None
        self._is_rubberband_drag = False
        
    def restoreSelection(self, indexes):
        sel_model = self.selectionModel()
        sel_model.clearSelection()
        for idx in indexes:
            sel_model.select(idx, QItemSelectionModel.Select | QItemSelectionModel.Rows)

    def keyPressEvent(self, event):
        if event.matches(QKeySequence.Copy):
            selected_rows = self.selectionModel().selectedRows()
            texts = []
            for idx in selected_rows:
                if self.parent_ref.current_view_mode in ("list", "history", "queue"):
                    song = self.parent_ref.songs_model.getSongItem(idx.row())
                else:
                    source_index = self.parent_ref.proxy_model.mapToSource(idx)
                    song = self.parent_ref.songs_model.getSongItem(source_index.row())
                if song:
                    texts.append(song.artist + " " + song.title)
            QApplication.clipboard().setText("\n".join(texts))
            return
        super().keyPressEvent(event)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._mousePressPos = event.position().toPoint()
            index = self.indexAt(event.position().toPoint())
            if index.isValid() and self.selectionModel().isSelected(index):
                self._drag_indexes = list(self.selectionModel().selectedRows())
            else:
                self._drag_indexes = []
            super().mousePressEvent(event)

        elif event.button() == Qt.RightButton:
            self.drag_start_position = event.position().toPoint()
            self._is_rubberband_drag = False

        else:
            super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if event.buttons() & Qt.LeftButton:
            if self._mousePressPos is not None and \
               (event.position().toPoint() - self._mousePressPos).manhattanLength() >= QApplication.startDragDistance():
                if self._drag_indexes:
                    self.startDragWithIndexes(self._drag_indexes)
                    self._drag_indexes = []
                    self._mousePressPos = None
                    return
                else:
                    indexes = self.selectionModel().selectedRows()
                    if indexes:
                        self.startDrag(Qt.MoveAction)
                        self._mousePressPos = None
                        return
            super().mouseMoveEvent(event)
            return

        if event.buttons() & Qt.RightButton and self.drag_start_position is not None:
            self._is_rubberband_drag = True
            rect = QRect(self.drag_start_position, event.position().toPoint()).normalized()
            self.rubber_band.setGeometry(rect)
            self.rubber_band.show()
            return

        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.RightButton:
            if self._is_rubberband_drag:
                self.rubber_band.hide()
                rect = self.rubber_band.geometry()
                top_left = self.indexAt(rect.topLeft())
                bottom_right = self.indexAt(rect.bottomRight())
                if top_left.isValid() and bottom_right.isValid():
                    selection = QItemSelection(top_left, bottom_right)
                    self.selectionModel().select(selection, QItemSelectionModel.ClearAndSelect | QItemSelectionModel.Rows)
            else:
                pt = event.position().toPoint()
                index = self.indexAt(pt)
                if index.isValid():
                    if not self.selectionModel().isSelected(index):
                        self.selectionModel().clearSelection()
                        self.selectionModel().select(index, QItemSelectionModel.Select | QItemSelectionModel.Rows)
                    self.parent_ref.onTableContextMenu(pt)

            self.drag_start_position = None
            self._is_rubberband_drag = False

        super().mouseReleaseEvent(event)

    def startDrag(self, supportedActions):
        indexes = self.selectionModel().selectedRows()
        if not indexes:
            return
        paths = []
        row_numbers = []
        for idx in indexes:
            current_model = self.model()
            if current_model == self.parent_ref.proxy_model:
                source_index = current_model.mapToSource(idx)
                song = self.parent_ref.songs_model.getSongItem(source_index.row())
                row_numbers.append(source_index.row())
            elif hasattr(current_model, "getSongItem"):
                song = current_model.getSongItem(idx.row())
                row_numbers.append(idx.row())
            else:
                song = None
            if song:
                paths.append(song.file_path)
        if not paths:
            return
        mime_data = QMimeData()
        mime_data.setText("\n".join(paths))
        mime_data.setData("application/x-qabstractitemmodeldatalist",
                          ",".join(str(r) for r in row_numbers).encode())
        drag = QDrag(self)
        drag.setMimeData(mime_data)
        drag.exec(Qt.MoveAction | Qt.CopyAction)

    def dropEvent(self, event):
        if self.parent_ref.current_view_mode in ("list", "history"):
            super().dropEvent(event)
            if self.parent_ref.current_list_name:
                self.parent_ref.saveListToFile(self.parent_ref.current_list_name,
                                                 self.parent_ref.songs_model.songs())
                if self.parent_ref.current_view_mode == "list":
                    self.parent_ref._backup_songs = self.parent_ref.songs_model.songs()[:]
        else:
            super().dropEvent(event)
    def startDragWithIndexes(self, indexes):
        if not indexes:
            return
        paths = []
        row_numbers = []
        for idx in indexes:
            current_model = self.model()
            if current_model == self.parent_ref.proxy_model:
                source_index = current_model.mapToSource(idx)
                song = self.parent_ref.songs_model.getSongItem(source_index.row())
                row_numbers.append(source_index.row())
            elif hasattr(current_model, "getSongItem"):
                song = current_model.getSongItem(idx.row())
                row_numbers.append(idx.row())
            else:
                song = None
            if song:
                paths.append(song.file_path)
        if not paths:
            return
        mime_data = QMimeData()
        mime_data.setText("\n".join(paths))
        mime_data.setData("application/x-qabstractitemmodeldatalist",
                        ",".join(str(r) for r in row_numbers).encode())
        drag = QDrag(self)
        drag.setMimeData(mime_data)
        drag.exec(Qt.MoveAction | Qt.CopyAction)
    
class SilenceDetectionWorker(QObject):
    finished = Signal(int, int, str)
    def __init__(self, audio_path: str):
        super().__init__()
        self.audio_path = audio_path
        self._is_cancelled = False

    def run(self):
        import librosa  
        if not os.path.exists(self.audio_path):
            self.finished.emit(0, 9999999, f"Audio file not found: {self.audio_path}")
            return
        try:
            y, sr = librosa.load(self.audio_path, sr=None, mono=True)
            hop_length = 1024
            frame_length = 2048
            rms = librosa.feature.rms(y=y, frame_length=frame_length, hop_length=hop_length)[0]
            thresh = 0.01 * max(rms)
            non_silence_indices = [i for i, val in enumerate(rms) if val > thresh]
            if not non_silence_indices:
                self.finished.emit(0, 0, "")
                return
            first_idx = non_silence_indices[0]
            last_idx = non_silence_indices[-1]
            ms_per_frame = 1000.0 * hop_length / sr
            intro_start_ms = int(first_idx * ms_per_frame)
            outro_start_ms = int((last_idx + 1) * ms_per_frame)
            self.finished.emit(intro_start_ms, outro_start_ms, "")
        except Exception as e:
            self.finished.emit(0, 9999999, str(e))

    def cancel(self):
        self._is_cancelled = True
        
class LibraryLoaderRunnable(QRunnable):
    def __init__(self, karaokePlayer, libName):
        super().__init__()
        self.karaokePlayer = karaokePlayer
        self.libName = libName
    def run(self):
        songs = self.karaokePlayer.db_fetch_library_songs(self.libName, sort_by_artist=True)
        if hasattr(self.karaokePlayer, "_current_library") and self.karaokePlayer._current_library == self.libName:
            QTimer.singleShot(0, lambda: self.karaokePlayer.updateLibrarySongs(songs))

class LazyAggregatedModel(QAbstractTableModel):
    def __init__(self, parent, db_path, library_map, letter_filter=None, chunk_size=200):
        super().__init__(parent)
        self.parent_ref = parent
        self.db_path = db_path
        self.library_map = library_map
        self.letter_filter = letter_filter
        self.chunk_size = chunk_size
        self.songs = []
        self.total_count = 0
        self.loaded_count = 0
        self.loadTotalCount()
    def loadTotalCount(self):
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        keys = list(self.library_map.keys())
        placeholders = ",".join(["?"] * len(keys))
        if self.letter_filter:
            c.execute(f"SELECT COUNT(*) FROM songs WHERE lib_name IN ({placeholders}) AND artist LIKE ?", tuple(keys) + (self.letter_filter + '%',))
        else:
            c.execute(f"SELECT COUNT(*) FROM songs WHERE lib_name IN ({placeholders})", tuple(keys))
        self.total_count = c.fetchone()[0]
        conn.close()
    def rowCount(self, parent=QModelIndex()):
        return self.loaded_count
    def columnCount(self, parent=QModelIndex()):
        return 4
    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if index.row() >= len(self.songs):
            return None
        if role in (Qt.DisplayRole, Qt.EditRole):
            song = self.songs[index.row()]
            col = index.column()
            if col == 0:
                return song.title
            elif col == 1:
                return song.artist
            elif col == 2:
                return ms_to_mmss(song.duration_ms)
            elif col == 3:
                return song.file_type.lstrip(".")
        if role == Qt.TextAlignmentRole:
            return Qt.AlignLeft | Qt.AlignVCenter
        return None
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            headers = ["Song", "Artist", "Duration", "File Type"]
            if section < len(headers):
                return headers[section]
            return ""
        return super().headerData(section, orientation, role)
    def canFetchMore(self, parent):
        if parent.isValid():
            return False
        return self.loaded_count < self.total_count
    def fetchMore(self, parent):
        if parent.isValid():
            return
        import sqlite3
        from pathlib import Path
        remaining = self.total_count - self.loaded_count
        to_fetch = min(self.chunk_size, remaining)
        if to_fetch <= 0:
            return
        start = self.loaded_count
        self.beginInsertRows(QModelIndex(), start, start + to_fetch - 1)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        keys = list(self.library_map.keys())
        placeholders = ",".join(["?"] * len(keys))
        case_str = "CASE lib_name " + " ".join([f"WHEN ? THEN {i}" for i in range(len(keys))]) + " END"
        if self.letter_filter:
            query = f"SELECT lib_name, filename, extension, artist, title, duration_ms FROM songs WHERE lib_name IN ({placeholders}) AND artist LIKE ? ORDER BY {case_str}, artist, title LIMIT ? OFFSET ?"
            params = keys + [self.letter_filter + '%'] + keys + [to_fetch, start]
        else:
            query = f"SELECT lib_name, filename, extension, artist, title, duration_ms FROM songs WHERE lib_name IN ({placeholders}) ORDER BY {case_str}, artist, title LIMIT ? OFFSET ?"
            params = keys + keys + [to_fetch, start]
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        for row in rows:
            lib_name, fn, ext, artist, title, dms = row
            folder = self.library_map.get(lib_name, "")
            full_path = str(Path(folder) / fn)
            si = SongItem(full_path, ext, artist, title, dms)
            self.songs.append(si)
        self.loaded_count += to_fetch
        self.endInsertRows()
    def getSongItem(self, row):
        if 0 <= row < len(self.songs):
            return self.songs[row]
        return None
    def setLetterFilter(self, letter):
        self.beginResetModel()
        self.letter_filter = letter
        self.songs = []
        self.loaded_count = 0
        self.loadTotalCount()
        self.endResetModel()
class LeftAlignDelegate(QStyledItemDelegate):
    def __init__(self, table_view):
        super().__init__(table_view)
        self.table_view = table_view

    def paint(self, painter, option, index):
        if index.row() == self.table_view.hovered_row:
            painter.save()
            painter.fillRect(option.rect, QColor("#2a2a2a"))
            painter.restore()
        if option.state & QStyle.State_Selected:
            painter.save()
            painter.fillRect(option.rect, QColor("#253511"))
            painter.restore()
        text = index.data(Qt.DisplayRole)
        r = option.rect
        if index.column() == 0:
            from pathlib import Path
            import os
            item_model = index.model()
            s = None
            if hasattr(item_model, "getSongItem"):
                s = item_model.getSongItem(index.row())
            else:
                source_index = self.table_view.parent_ref.proxy_model.mapToSource(index)
                s = self.table_view.parent_ref.songs_model.getSongItem(source_index.row())
            if s:
                ext = s.file_type.casefold()
                if ext in (".mp4", ".mkv", ".avi", ".cdg"):
                    thumb_path = os.path.join("thumbs", os.path.basename(s.file_path) + ".jpg")
                    if os.path.exists(thumb_path):
                        pix = QPixmap(thumb_path)
                        if not pix.isNull():
                            fixed_pix = pix.scaled(50, 29, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                            pix_x = r.left()
                            pix_y = r.top() + (r.height() - fixed_pix.height()) // 2
                            painter.drawPixmap(pix_x, pix_y, fixed_pix)
                            text_rect = QRect(r.left() + fixed_pix.width() + 8, r.top(), r.width() - fixed_pix.width() - 5, r.height())
                            painter.setPen(QColor("#FFFFFF"))
                            painter.drawText(text_rect, Qt.AlignVCenter | Qt.AlignLeft, str(text))
                            return
        painter.save()
        painter.setPen(QColor("#FFFFFF"))
        text_rect = option.rect.adjusted(5, 0, -5, 0)
        painter.drawText(text_rect, Qt.AlignVCenter | Qt.AlignLeft, str(text) if text else "")
        painter.restore()

class KaraokePlayer(QMainWindow):
    search_results_ready = Signal()
    library_load_complete = Signal()
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(1200,700)
        self.setWindowIcon(QIcon(resource_path("ico.ico")))
        self.settings = QSettings(SETTINGS_FILE, QSettings.IniFormat)
        self.restoreGeometry(self.settings.value("windowGeometry", b""))
        self.restoreState(self.settings.value("windowState", b""))
        self.temp_folder = Path("temp")
        if self.settings.value("autoDeleteTemp") is None:
            self.settings.setValue("autoDeleteTemp", True)
        if self.settings.value("autoDeleteTemp", True, type=bool):
            if self.temp_folder.exists():
                shutil.rmtree(self.temp_folder)
            self.temp_folder.mkdir(exist_ok=True)
        else:
            if not self.temp_folder.exists():
                self.temp_folder.mkdir(exist_ok=True)
        self.library_data = {}
        self.library_map = {}
        self.user_lists = {}
        self.current_queue = []
        self.current_play_index = -1
        self.current_view_mode = "library"
        self.waiting_for_render = False
        self.current_list_name = None
        self.grouped_mode = False
        self.aggregated_grouping = True
        self.combined_shift_processing = False
        self.combined_shift_thread = None
        self.pending_render_requests = []
        self._user_seeking = False
        self._processing_request = None
        self._current_rendering_item = None
        self.randomIdleEnabled = True
        self.idleChangeTimer = QTimer(self)
        self.render_queue = []
        self.idleChangeTimer.setSingleShot(True)
        self.idleChangeTimer.timeout.connect(self._onIdleChangeTimer)
        self.defaultIdle = self.settings.value("defaultIdle", "wire.mp4")
        self.idleChangeInterval = self.settings.value("idleChangeInterval", 30, type=int)
        self.idles_folder = Path(IDLES_FOLDER)
        self.idles_folder.mkdir(exist_ok=True)
        self.idle_videos = sorted([f.name for f in self.idles_folder.glob("*.mp4")])
        self.conn = sqlite3.connect("library.db", timeout=10)
        c = self.conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS songs (lib_name TEXT NOT NULL, filename TEXT NOT NULL, extension TEXT, artist TEXT, title TEXT, duration_ms INTEGER, PRIMARY KEY(lib_name, filename))")
        c.execute("CREATE INDEX IF NOT EXISTS idx_artist_title ON songs (artist, title)")
        c.execute("CREATE TABLE IF NOT EXISTS libraries (lib_name TEXT PRIMARY KEY, paths TEXT, sort_index INTEGER DEFAULT 0)")
        self.conn.commit()
        self.loadLibraryPaths()
        self.loadUserLists()
        self.video_player = QMediaPlayer()
        self.video_audio_output = QAudioOutput()
        self.video_audio_output.setVolume(0.0)
        self.video_player.setAudioOutput(self.video_audio_output)
        self.audio_player_preset = QMediaPlayer()
        self.audio_output_preset = QAudioOutput()
        self.audio_output_preset.setVolume(1.0)
        self.audio_player_preset.setAudioOutput(self.audio_output_preset)
        self.second_window = None
        self.sync_timer = QTimer(self)
        self.sync_timer.setInterval(100)
        self.sync_timer.timeout.connect(self.syncPlayers)
        self.sync_timer.start()
        self.update_timer = QTimer(self)
        self.update_timer.setInterval(500)
        self.update_timer.timeout.connect(self.updatePlayerUI)
        self.update_timer.start()
        self._search_timer = QTimer(self)
        self._search_timer.setSingleShot(True)
        self._search_timer.setInterval(300)
        self._search_timer.timeout.connect(self.doUpdateFilter)
        self.video_player.mediaStatusChanged.connect(self.onMediaStatusChanged)
        self.video_player.errorOccurred.connect(self.onPlaybackError)
        self.audio_player_preset.errorOccurred.connect(self.onPlaybackError)
        self.initUI()
        self.setupShortcuts()
        self.loadIdleVideo()
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "LibraryCategory":
                self.onCategoryClicked(item)
                break

    def showEvent(self, event):
        super().showEvent(event)
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "LibraryCategory":
                self.categories_list.setCurrentItem(item)
                self.onCategoryClicked(item)
                break
        QTimer.singleShot(500, self.adjustAlphabetPanelWidth)
    def updateSongDurationIfNeeded(self, song_item: SongItem):
        if song_item.duration_ms == 0:
            if song_item.lib_name:
                conn = sqlite3.connect("library.db")
                c = conn.cursor()
                c.execute("SELECT duration_ms FROM songs WHERE lib_name=? AND filename=?",
                        (song_item.lib_name, os.path.basename(song_item.file_path)))
                row = c.fetchone()
                conn.close()
                if row and row[0] != 0:
                    song_item.duration_ms = row[0]
                    song_item.duration_str = ms_to_mmss(song_item.duration_ms)

            if song_item.duration_ms == 0:
                dur = top_level_get_duration(song_item.file_path)
                song_item.duration_ms = dur
                song_item.duration_str = ms_to_mmss(dur)

                if song_item.lib_name and dur > 0:
                    conn = sqlite3.connect("library.db")
                    c = conn.cursor()
                    c.execute("""UPDATE songs
                                SET duration_ms=?
                                WHERE lib_name=? AND filename=?""",
                            (dur, song_item.lib_name, os.path.basename(song_item.file_path)))
                    conn.commit()
                    conn.close()

    def changeEvent(self, event):
        super().changeEvent(event)
        if event.type() == QEvent.WindowStateChange:
            if not self.isMaximized():
                QTimer.singleShot(0, lambda: self.horizontal_splitter.setSizes([self.left_panel.width(), self.width() - self.left_panel.width()]))

    def adjustAlphabetPanelWidth(self):
        vsb = self.alphabet_panel.verticalScrollBar()
        extra = vsb.sizeHint().width() if (vsb.isVisible() or vsb.maximum() > 0) else 0
        self.alphabet_panel.setFixedWidth(40 + extra)

    def aggregateLibraries(self):
        master_list = []
        for lib in self.library_map.keys():
            master_list.extend(self.db_fetch_library_songs(lib, sort_by_artist=True))
        return master_list

    def _onIdleChangeTimer(self):
        if not self.current_queue:
            if self.idle_videos:
                current_source = self.video_player.source().toLocalFile() if self.video_player.source() else ""
                new_idle = random.choice(self.idle_videos)
                if current_source.endswith(new_idle) and len(self.idle_videos) > 1:
                    choices = [v for v in self.idle_videos if v != new_idle]
                    new_idle = random.choice(choices)
                idle_video_path = str(self.idles_folder / new_idle)
                self.video_player.stop()
                self.video_player.setSource(QUrl.fromLocalFile(idle_video_path))
                try:
                    self.video_player.setLoops(QMediaPlayer.Loops.Infinite)
                except Exception:
                    pass
                self.video_player.play()
                if self.second_window:
                    self.second_window.player.stop()
                    self.second_window.player.setSource(QUrl.fromLocalFile(idle_video_path))
                    try:
                        self.second_window.player.setLoops(QMediaPlayer.Loops.Infinite)
                    except Exception:
                        pass
                    self.second_window.player.play()
        if self.idleChangeInterval > 0:
            self.idleChangeTimer.start(self.idleChangeInterval * 1000)

    def loadIdleVideo(self):
        idle_video_name = self.settings.value("defaultIdle", "wire.mp4")
        idle_video_path = str(self.idles_folder / idle_video_name)
        if not os.path.exists(idle_video_path):
            idle_video_path = resource_path("idle.mp4")
        self.video_player.stop()
        self.video_player.setSource(QUrl.fromLocalFile(idle_video_path))
        try:
            self.video_player.setLoops(QMediaPlayer.Loops.Infinite)
        except Exception:
            pass
        self.video_player.play()
        if self.second_window:
            self.second_window.player.stop()
            self.second_window.player.setSource(QUrl.fromLocalFile(idle_video_path))
            try:
                self.second_window.player.setLoops(QMediaPlayer.Loops.Infinite)
            except Exception:
                pass
            self.second_window.player.play()
        if self.idleChangeInterval > 0:
            self.idleChangeTimer.start(self.idleChangeInterval * 1000)
    def start_background_library_load(self):
        threading.Thread(target=self.load_all_libraries, daemon=True).start()
    def load_all_libraries(self):
        for lib_name, folder in self.library_map.items():
            p = Path(folder)
            found_files = []
            for ext in SUPPORTED_FILE_EXTENSIONS:
                found_files.extend(p.rglob(f"*{ext}"))
            for f in found_files:
                fn = f.name
                extension = f.suffix.casefold()
                artist, title = parse_filename_for_artist_song(fn)
                dur = self.getDurationWithFfprobe(str(f))
                self.db_add_song(lib_name, fn, extension, artist, title, dur)
        self.library_load_complete.emit()
    def onLibraryLoadComplete(self):
        self.buildCategories()
        self.hideHistorySubitems()
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "LibraryCategory":
                self.onCategoryClicked(item)
                break
    def _updateMasterList(self, master_list):
        self.songs_model.show_key_tempo = False
        self.songs_model.setSongs(master_list)
        self.grouped_mode = True
        self.table_view.setModel(self.songs_model)
        self.table_view.setSortingEnabled(False)
        self.table_view.horizontalHeader().setSortIndicator(-1, Qt.AscendingOrder)
        self.updateTableViewMode()

    def asyncLoadLibraries(self):
        pass
    def toggleHistoryExpansion(self):
        expanded = False
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "HistorySub" and not item.isHidden():
                expanded = True
                break
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "HistorySub":
                item.setHidden(expanded)
    def onHeaderClicked(self, section):
        model = self.table_view.model()
        if isinstance(model, LazyLibraryModel):
            if model.lib_name is None:
                self.aggregated_grouping = False
            current_section = model.sort_column
            current_order = model.sort_order
            if current_section == section:
                new_order = Qt.DescendingOrder if current_order == Qt.AscendingOrder else Qt.AscendingOrder
            else:
                new_order = Qt.AscendingOrder
            self.table_view.horizontalHeader().setSortIndicator(section, new_order)
            model.setSortColumn(section, new_order)
            self.table_view.scrollToTop()
        else:
            if self.table_view.isSortingEnabled():
                self.table_view.sortByColumn(section, self.table_view.horizontalHeader().sortIndicatorOrder())

    def onCategoriesContextMenu(self, pos):
        item = self.categories_list.itemAt(pos)
        if not item:
            return
        role = item.data(Qt.UserRole)
        txt = item.text().strip()
        menu = QMenu(self)
        if role == "ListSub":
            act_delete = menu.addAction("Delete this list")
            chosen = menu.exec(self.categories_list.mapToGlobal(pos))
            if chosen == act_delete:
                resp = QMessageBox.question(self, "Delete List", "Are you sure you want to delete the list '" + txt + "'?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
                if resp == QMessageBox.Yes:
                    self.deleteListFile(txt)
                return
        if role == "LibrarySub":
            act_remove = menu.addAction("Remove library from player")
            act_edit = menu.addAction("Edit library")
            act_move_up = None
            act_move_down = None
            act_rescan = menu.addAction("Rescan library")
            act_scan_duration = menu.addAction("Run scan for song durations")
            act_regen_thumb = menu.addAction("Force regenerate thumbnails")
            lib_items = []
            for i in range(self.categories_list.count()):
                it = self.categories_list.item(i)
                if it.data(Qt.UserRole) == "LibrarySub":
                    lib_items.append(it)
            index_in_libs = lib_items.index(item)
            if index_in_libs > 0:
                act_move_up = menu.addAction("Move up")
            if index_in_libs < len(lib_items) - 1:
                act_move_down = menu.addAction("Move down")
            chosen = menu.exec(self.categories_list.mapToGlobal(pos))
            if chosen == act_remove:
                resp = QMessageBox.question(self, "Remove Library", "Are you sure you want to remove the library '" + txt + "' from the player?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
                if resp == QMessageBox.Yes:
                    self.removeLibrary(txt)
            elif chosen == act_edit:
                self.showEditLibraryDialog(txt)
            elif act_move_up and chosen == act_move_up:
                self.moveLibraryItem(item, up=True)
            elif act_move_down and chosen == act_move_down:
                self.moveLibraryItem(item, up=False)
            elif chosen == act_rescan:
                resp = QMessageBox.question(self, "Rescan Library", "Are you sure you want to rescan the library '" + txt + "'?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
                if resp == QMessageBox.Yes:
                    folder = self.library_map.get(txt, "")
                    if folder:
                        from PySide6.QtCore import QThreadPool
                        QThreadPool.globalInstance().start(LibraryLoaderRunnable(self, txt))
                        import threading
                        threading.Thread(target=self.scanAndStoreLibrary, args=(txt, folder), daemon=True).start()
            elif chosen == act_scan_duration:
                self.scan_durations_for_library(txt)
            elif chosen == act_regen_thumb:
                reply = QMessageBox.question(self, "Force regenerate thumbnails", "Are you sure you want to regenerate thumbnails for this library?", QMessageBox.Yes | QMessageBox.Cancel, QMessageBox.Cancel)
                if reply == QMessageBox.Yes:
                    folder = self.library_map.get(txt, "")
                    if folder:
                        from pathlib import Path
                        import os
                        for f in Path(folder).rglob("*"):
                            if f.suffix.lower() in SUPPORTED_FILE_EXTENSIONS:
                                thumb_path = os.path.join("thumbs", f.name + ".jpg")
                                if os.path.exists(thumb_path):
                                    os.remove(thumb_path)
                                createThumbnail(str(f))
                        QMessageBox.information(self, "Thumbnails regenerated", "Thumbnails have been regenerated for this library.")
            return
        if role == "HistoryCategory":
            self.toggleHistoryExpansion()
            return
        if role == "QueueCategory":
            act_shuffle = menu.addAction("Shuffle Queue")
            act_clear = menu.addAction("Clear Queue")
            chosen = menu.exec(self.categories_list.mapToGlobal(pos))
            if chosen == act_shuffle:
                self.shuffleQueue()
            elif chosen == act_clear:
                self.current_queue.clear()
                self.current_play_index = -1
                self.video_player.stop()
                self.audio_player_preset.stop()
                if self.current_view_mode == "queue":
                    self.songs_model.setSongs(self.current_queue)
            return

    def moveLibraryItem(self, item, up=True):
        row = self.categories_list.row(item)
        target = row - 1 if up else row + 1
        if target < 0 or target >= self.categories_list.count():
            return
        self.categories_list.takeItem(row)
        self.categories_list.insertItem(target, item)
        self.updateLibraryOrder()
    def updateLibraryOrder(self):
        conn = sqlite3.connect('library.db')
        c = conn.cursor()
        order = 0
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "LibrarySub":
                libname = item.text().strip()
                libname = libname.lstrip()
                c.execute("UPDATE libraries SET sort_index=? WHERE lib_name=?", (order, libname))
                order += 1
        conn.commit()
        conn.close()
    def onCategoryClicked(self, item):
        self.setQueueRowActive(False)
        role = item.data(Qt.UserRole)
        text = item.text().strip()
        if role == 'QueueCategory':
            self.current_view_mode = 'queue'
            self.current_list_name = None
            self.songs_model.history_mode = False
            self.setQueueRowActive(True)
            self.showQueue()
        elif role == 'LibrarySub':
            self.aggregated_grouping = True
            self.current_view_mode = 'library'
            self.current_list_name = None
            self.songs_model.history_mode = False
            self.songs_model.show_key_tempo = False
            self.grouped_mode = False
            self.setupLazyLibrary(text, None)
            self.updateAlphabetPanel(text)
        elif role == 'LibraryCategory':
            self.aggregated_grouping = True
            self.current_view_mode = 'library'
            self.current_list_name = None
            self.songs_model.history_mode = False
            self.songs_model.show_key_tempo = False
            self.grouped_mode = False
            self.setupLazyLibrary(None, None)
            self.updateAlphabetPanel(None)
        elif role == 'ListSub':
            self.current_view_mode = 'list'
            self.current_list_name = text
            self.songs_model.history_mode = False
            self.songs_model.list_mode = True
            songs = self.loadListFromFile(text)
            self.songs_model.show_key_tempo = True
            self.songs_model.setSongs(songs)
            self._backup_songs = songs[:]
            self.updateTableViewMode()
        elif role == 'HistorySub':
            self.showHistory(text)

    def onCategoryDoubleClicked(self, item):
        self.onCategoryClicked(item)
    def addFolder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Library Folder")
        if folder:
            text_lines = folder
            def_name = os.path.basename(folder)
            self.showEditLibraryDialog(None, def_name, text_lines)
    def removeLibrary(self, lib_name):
        if lib_name in self.library_map:
            del self.library_map[lib_name]
        conn = sqlite3.connect('library.db')
        c = conn.cursor()
        c.execute("DELETE FROM songs WHERE lib_name = ?", (lib_name,))
        c.execute("DELETE FROM libraries WHERE lib_name = ?", (lib_name,))
        conn.commit()
        conn.close()
        cleanThumbs()  
        self.buildCategories()
        self.hideHistorySubitems()

    def increaseKey(self):
        selected = self.getSelectedTrack()
        if not selected:
            return
        selected.key_change += 1
        self.updateKeyTempoLabels(selected)
        self.songs_model.layoutChanged.emit()

        if self.current_view_mode == "list" and self.current_list_name:
            self.saveListToFile(self.current_list_name, self.songs_model.songs())

    def decreaseKey(self):
        selected = self.getSelectedTrack()
        if not selected:
            return
        selected.key_change -= 1
        self.updateKeyTempoLabels(selected)
        self.songs_model.layoutChanged.emit()
        if self.current_view_mode == "list" and self.current_list_name:
            self.saveListToFile(self.current_list_name, self.songs_model.songs())

    def increaseTempo(self):
        selected = self.getSelectedTrack()
        if not selected:
            return
        selected.tempo_change += 1
        self.updateKeyTempoLabels(selected)
        self.songs_model.layoutChanged.emit()
        if self.current_view_mode == "list" and self.current_list_name:
            self.saveListToFile(self.current_list_name, self.songs_model.songs())

    def decreaseTempo(self):
        selected = self.getSelectedTrack()
        if not selected:
            return
        selected.tempo_change -= 1
        self.updateKeyTempoLabels(selected)
        self.songs_model.layoutChanged.emit()
        if self.current_view_mode == "list" and self.current_list_name:
            self.saveListToFile(self.current_list_name, self.songs_model.songs())

    def db_init(self):
        pass
    def db_add_song(self, lib_name, filename, extension, artist, title, duration_ms):
        key = (lib_name, filename)
        self.library_data[key] = {
            "extension": extension,
            "artist": artist,
            "title": title,
            "duration_ms": duration_ms
        }
    def db_remove_library(self, lib_name):
        import sqlite3
        conn = sqlite3.connect('library.db')
        c = conn.cursor()
        c.execute("DELETE FROM songs WHERE lib_name = ?", (lib_name,))
        conn.commit()
        conn.close()

    def db_fetch_library_songs(self, lib_name, sort_by_artist=True):
        import sqlite3
        conn = sqlite3.connect('library.db')
        c = conn.cursor()
        if sort_by_artist:
            c.execute("SELECT filename, extension, artist, title, duration_ms FROM songs WHERE lib_name = ? ORDER BY artist, title", (lib_name,))
        else:
            c.execute("SELECT filename, extension, artist, title, duration_ms FROM songs WHERE lib_name = ?", (lib_name,))
        rows = c.fetchall()
        conn.close()
        folder = self.library_map.get(lib_name, "")
        items = []
        for row in rows:
            fn, ext, artist, title, dms = row
            full_path = str(Path(folder) / fn)
            si = SongItem(full_path, ext, artist, title, dms)
            items.append(si)
        return items
    def showEditLibraryDialog(self, existing_name=None, default_name=None, default_paths=None):
        d = QDialog(self)
        d.setWindowTitle("Edit Library")
        layout = QVBoxLayout(d)
        name_label = QLabel("Library name:")
        layout.addWidget(name_label)
        name_edit = QLineEdit()
        if existing_name:
            name_edit.setText(existing_name)
        elif default_name:
            name_edit.setText(default_name)
        layout.addWidget(name_edit)
        loc_label = QLabel("Location(s). One folder path per line. First valid is used:")
        layout.addWidget(loc_label)
        loc_text = QTextEdit()
        if existing_name:
            conn = sqlite3.connect("library.db")
            c = conn.cursor()
            c.execute("SELECT paths FROM libraries WHERE lib_name=?", (existing_name,))
            row = c.fetchone()
            conn.close()
            if row and row[0]:
                loc_text.setText(row[0])
        elif default_paths:
            loc_text.setText(default_paths)
        layout.addWidget(loc_text)
        btns = QHBoxLayout()
        save_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        btns.addWidget(save_btn)
        btns.addWidget(cancel_btn)
        layout.addLayout(btns)
        def onSave():
            new_name = name_edit.text().strip()
            if not new_name:
                QMessageBox.warning(d, "Invalid Name", "Library name cannot be empty.")
                return
            conn2 = sqlite3.connect("library.db")
            c2 = conn2.cursor()
            if existing_name and new_name != existing_name:
                c2.execute("SELECT lib_name FROM libraries WHERE lib_name=?", (new_name,))
                row2 = c2.fetchone()
                if row2:
                    QMessageBox.warning(d, "Name Exists", "A library with this name already exists.")
                    return
                c2.execute("UPDATE songs SET lib_name=? WHERE lib_name=?", (new_name, existing_name))
                c2.execute("UPDATE libraries SET lib_name=? WHERE lib_name=?", (new_name, existing_name))
            elif not existing_name:
                c2.execute("SELECT lib_name FROM libraries WHERE lib_name=?", (new_name,))
                row2 = c2.fetchone()
                if row2:
                    QMessageBox.warning(d, "Name Exists", "A library with this name already exists.")
                    return
                c2.execute("SELECT MAX(sort_index) FROM libraries")
                max_sort = c2.fetchone()[0]
                if max_sort is None:
                    max_sort = 0
                else:
                    max_sort = int(max_sort)
                new_sort_index = max_sort + 1
                c2.execute("INSERT INTO libraries (lib_name, paths, sort_index) VALUES (?, ?, ?)", (new_name, "", new_sort_index))
            updated_paths = loc_text.toPlainText().strip()
            c2.execute("UPDATE libraries SET paths=? WHERE lib_name=?", (updated_paths, new_name))
            conn2.commit()
            conn2.close()
            if not existing_name:
                self.scanMultiplePathsAndPopulate(new_name, updated_paths)
                self._showScanPrompt(new_name)
            d.accept()
            self.loadLibraryPaths()
            self.buildCategories()
            self.hideHistorySubitems()
        save_btn.clicked.connect(onSave)
        cancel_btn.clicked.connect(d.reject)
        d.exec()

    def scanMultiplePathsAndPopulate(self, lib_name, multiline_paths):
        path_lines = [p.strip() for p in multiline_paths.splitlines() if p.strip()]
        conn = sqlite3.connect("library.db")
        c = conn.cursor()
        c.execute("DELETE FROM songs WHERE lib_name=?", (lib_name,))
        for single_path in path_lines:
            p = Path(single_path)
            if p.is_dir():
                found_files = []
                for ext in SUPPORTED_FILE_EXTENSIONS:
                    found_files.extend(p.rglob("*" + ext))
                for f in found_files:
                    fn = f.name
                    extension = f.suffix.casefold()
                    artist, title = parse_filename_for_artist_song(fn)
                    c.execute("INSERT INTO songs (lib_name, filename, extension, artist, title, duration_ms) VALUES (?,?,?,?,?,?)", (lib_name, fn, extension, artist, title, 0))
        conn.commit()
        conn.close()

    def _showScanPrompt(self, lib_name):
        dlg = QDialog(self)
        dlg.setWindowTitle("Scan Durations?")
        layout = QVBoxLayout(dlg)
        label = QLabel("Scan new library for song durations now?\nYou can also run it later by right-clicking on the library.")
        layout.addWidget(label)
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        run_btn = QPushButton("Run now")
        later_btn = QPushButton("Later")
        run_btn.setMinimumSize(100, 40)
        later_btn.setMinimumSize(100, 40)
        btn_layout.addWidget(run_btn)
        btn_layout.addWidget(later_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        dlg.setLayout(layout)
        run_btn.clicked.connect(lambda: (dlg.accept(), self.scan_durations_for_library(lib_name)))
        later_btn.clicked.connect(dlg.reject)
        dlg.show()

    def scanAndStoreLibrary(self, library_name, folder):
        import sqlite3
        from pathlib import Path
        
        p = Path(folder)
        conn = sqlite3.connect("library.db")
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS songs
            (lib_name TEXT, filename TEXT, extension TEXT, artist TEXT, title TEXT, duration_ms INTEGER)
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_lib_name ON songs (lib_name)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_artist_title ON songs (artist, title)")
        
        existing_data = {}
        c.execute("SELECT filename, duration_ms FROM songs WHERE lib_name = ?", (library_name,))
        for row in c.fetchall():
            filename, old_dur = row
            existing_data[filename] = old_dur if old_dur else 0

        found_files = []
        for ext in SUPPORTED_FILE_EXTENSIONS:
            found_files.extend(p.rglob("*" + ext))
        found_files = list(found_files)

        lib_item = None
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "LibrarySub":
                txt = item.text().strip()
                if txt == library_name:
                    lib_item = item
                    break
        total_files = len(found_files)
        processed_count = 0
        if lib_item:
            lib_item.setText(f"         {library_name} (0/{total_files})")

        found_filenames = set()

        for f in found_files:
            fn = f.name
            extension = f.suffix.casefold()
            artist, title = parse_filename_for_artist_song(fn)
            old_duration = existing_data.get(fn, 0)  

            c.execute("""
                INSERT OR REPLACE INTO songs
                (lib_name, filename, extension, artist, title, duration_ms)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (library_name, fn, extension, artist, title, old_duration))

            found_filenames.add(fn)
            processed_count += 1
            if lib_item:
                lib_item.setText(f"         {library_name} ({processed_count}/{total_files})")

        if found_filenames:
            placeholders = ",".join("?" for _ in found_filenames)
            c.execute(
                f"DELETE FROM songs WHERE lib_name=? AND filename NOT IN ({placeholders})",
                (library_name, *found_filenames)
            )
        else:
            c.execute("DELETE FROM songs WHERE lib_name=?", (library_name,))
        conn.commit()
        conn.close()

        cleanThumbs() 

        songs = self.db_fetch_library_songs(library_name, sort_by_artist=True)
        QTimer.singleShot(0, lambda: self.updateLibrarySongs(songs))
        if lib_item:
            lib_item.setText(f"         {library_name}")

    def scan_durations_for_library(self, library_name):
        import sqlite3
        from pathlib import Path
        conn = sqlite3.connect("library.db", timeout=10)
        c = conn.cursor()
        c.execute("SELECT filename, extension FROM songs WHERE lib_name = ? AND duration_ms = 0", (library_name,))
        zero_duration_files = c.fetchall()
        conn.close()
        if not zero_duration_files:
            QMessageBox.information(self, "No durations to scan", "All songs in this library have a duration.")
            return
        total = len(zero_duration_files)
        dlg = QProgressDialog("Scanning durations...", "Cancel", 0, total, self)
        dlg.setWindowTitle("Scan Durations")
        dlg.setWindowModality(Qt.NonModal)
        dlg.setFixedWidth(400)
        dlg.setValue(0)
        canceled = False
        conn = sqlite3.connect("library.db", timeout=10)
        c = conn.cursor()
        for i, (fn, ext) in enumerate(zero_duration_files, start=1):
            if dlg.wasCanceled():
                canceled = True
                break
            dlg.setLabelText("Scanning " + fn)
            dlg.setValue(i)
            full_path = str(Path(self.library_map[library_name]) / fn)
            if ext.casefold() == ".cdg" and full_path.casefold().endswith(".cdg"):
                full_path = full_path[:-4] + ".mp3"
            dur = top_level_get_duration(full_path)
            c.execute("UPDATE songs SET duration_ms = ? WHERE lib_name = ? AND filename = ?", (dur, library_name, fn))
            if ext.casefold() in (".mp4", ".mkv", ".avi", ".cdg"):
                createThumbnail(str(Path(self.library_map[library_name]) / fn))
            QApplication.processEvents()
        conn.commit()
        conn.close()
        dlg.close()
        if not canceled:
            QMessageBox.information(self, "Done", "Duration scan complete!")
        else:
            QMessageBox.information(self, "Canceled", "Duration scan was canceled.")

    def getDurationWithFfprobe(self, path_str):
        if not os.path.exists(path_str):
            return 0
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path_str
        ]
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, encoding='utf-8', errors='replace', startupinfo=startupinfo)
        if result.returncode == 0:
            try:
                length = float(result.stdout.strip())
                return int(length * 1000)
            except Exception:
                return 0
        return 0

    def initUI(self):
        if QApplication.instance():
            QApplication.instance().setStyleSheet(
                QApplication.instance().styleSheet() +
                " QMenu { background-color: #181818; color: #FFFFFF; border: 1px solid #333333; }"
                " QMenu::item:selected { background-color: #2a2a2a; border: none; }"
            )
        self.setContentsMargins(0, 0, 0, 0)
        main_widget = QWidget()
        main_layout = QHBoxLayout(main_widget)
        main_layout.setContentsMargins(10, 10, 0, 0)
        main_layout.setSpacing(0)
        self.setCentralWidget(main_widget)

        self.horizontal_splitter = QSplitter(Qt.Horizontal, self)
        main_layout.addWidget(self.horizontal_splitter)

        self.left_panel = QWidget()
        lp_layout = QVBoxLayout(self.left_panel)
        lp_layout.setContentsMargins(0, 0, 0, 0)
        lp_layout.setSpacing(0)

        self.left_splitter = QSplitter(Qt.Vertical)
        
        self.video_container = QWidget()
        video_grid = QGridLayout(self.video_container)
        video_grid.setContentsMargins(0, 0, 0, 0)
        video_grid.setSpacing(0)

        self.video_widget = CustomVideoWidget()
        self.video_player.setVideoOutput(self.video_widget)
        self.video_widget.doubleClicked.connect(self.toggleFullscreen)

        self.video_overlay = CDGOverlayWidget(self.video_container)
        self.video_overlay.raise_()
        self.video_overlay.show()

        video_grid.addWidget(self.video_widget, 0, 0)
        video_grid.addWidget(self.video_overlay, 0, 0)
        self.video_container.setLayout(video_grid)

        self.left_splitter.addWidget(self.video_container)

        self.bottom_fixed = QWidget()
        bf_layout = QVBoxLayout(self.bottom_fixed)
        bf_layout.setContentsMargins(0, 0, 0, 0)
        bf_layout.setSpacing(4)

        seek_row = QWidget()
        sr_layout = QHBoxLayout(seek_row)
        sr_layout.setContentsMargins(10, 2, 10, 2)
        sr_layout.setSpacing(8)

        self.lbl_current_time = QLabel("0:00")
        self.lbl_current_time.setFixedWidth(40)
        self.lbl_current_time.setAlignment(Qt.AlignCenter)
        self.lbl_current_time.setStyleSheet("QLabel { color: #FFFFFF; }")
        sr_layout.addWidget(self.lbl_current_time)

        self.seek_slider = QSlider(Qt.Horizontal)
        self.seek_slider.setRange(0, 1000)
        self.seek_slider.sliderPressed.connect(self.onSeekPress)
        self.seek_slider.sliderReleased.connect(self.onSeekRelease)
        self.seek_slider.sliderMoved.connect(self.onSeekMove)
        self.seek_slider.setStyleSheet(
            "QSlider::groove:horizontal { background: #404040; height: 6px; border-radius: 3px; }"
            "QSlider::sub-page:horizontal { background: #aae129; height: 6px; border-radius: 3px; margin: 0px; }"
            "QSlider::add-page:horizontal { background: #181818; height: 6px; border-radius: 3px; margin: 0px; }"
            "QSlider::handle:horizontal { background: #ffffff; border: 1px solid #333333; width: 10px; height: 10px; margin: -4px 0; border-radius: 4px; }"
        )
        sr_layout.addWidget(self.seek_slider, 1)

        self.lbl_total_time = QLabel("0:00")
        self.lbl_total_time.setFixedWidth(40)
        self.lbl_total_time.setAlignment(Qt.AlignCenter)
        self.lbl_total_time.setStyleSheet("QLabel { color: #FFFFFF; }")
        sr_layout.addWidget(self.lbl_total_time)

        seek_row.setLayout(sr_layout)
        bf_layout.addWidget(seek_row)

        control_bar = QWidget()
        cb_layout = QHBoxLayout(control_bar)
        cb_layout.setContentsMargins(10, 0, 10, 0)
        cb_layout.setSpacing(0)

        left_widget = QWidget()
        left_layout = QHBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(10)

        self.btn_play_pause_left = QPushButton("")
        self.btn_play_pause_left.setFixedSize(48, 48)
        self.play_icon = self.style().standardIcon(QStyle.SP_MediaPlay)
        self.btn_play_pause_left.setIcon(self.play_icon)
        self.btn_play_pause_left.setIconSize(QSize(24, 24))
        self.btn_play_pause_left.clicked.connect(self.playPause)
        self.btn_play_pause_left.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: none; } QPushButton:hover { background-color: #333333; }")
        left_layout.addWidget(self.btn_play_pause_left)

        self.btn_next_left = QPushButton("")
        self.btn_next_left.setFixedSize(48, 48)
        skip_icon = self.style().standardIcon(QStyle.SP_MediaSkipForward)
        self.btn_next_left.setIcon(skip_icon)
        self.btn_next_left.setIconSize(QSize(24, 24))
        self.btn_next_left.clicked.connect(self.playNext)
        self.btn_next_left.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: none; } QPushButton:hover { background-color: #333333; }")
        left_layout.addWidget(self.btn_next_left)

        monitors_icon = self.style().standardIcon(QStyle.SP_ComputerIcon)
        self.btn_monitor_left = QPushButton("")
        self.btn_monitor_left.setFixedSize(48, 48)
        self.btn_monitor_left.setIcon(monitors_icon)
        self.btn_monitor_left.setIconSize(QSize(24, 24))
        self.btn_monitor_left.setCheckable(True)
        self.btn_monitor_left.setToolTip("Pop out video to second window")
        self.btn_monitor_left.clicked.connect(self.toggleSecondScreenPopout)
        self.btn_monitor_left.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: none; } QPushButton:hover { background-color: #333333; }")
        left_layout.addWidget(self.btn_monitor_left)

        left_widget.setLayout(left_layout)
        cb_layout.addWidget(left_widget)
        cb_layout.addStretch()

        right_widget = QWidget()
        right_widget.setStyleSheet("QWidget { border: 1px solid #333333; background-color: #242424; }")
        keytempo_layout = QGridLayout(right_widget)
        keytempo_layout.setContentsMargins(0, 0, 0, 0)
        keytempo_layout.setSpacing(0)

        label_key_top = QLabel("Key:")
        label_key_top.setStyleSheet("QLabel { color: #FFFFFF; }")
        keytempo_layout.addWidget(label_key_top, 0, 0)

        self.btn_key_plus = QPushButton("+")
        self.btn_key_plus.setFixedSize(24, 24)
        self.btn_key_plus.clicked.connect(self.increaseKey)
        self.btn_key_plus.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: 1px solid #333333; } QPushButton:hover { background-color: #333333; }")
        keytempo_layout.addWidget(self.btn_key_plus, 0, 1)

        label_tempo_top = QLabel("Tempo:")
        label_tempo_top.setStyleSheet("QLabel { color: #FFFFFF; }")
        keytempo_layout.addWidget(label_tempo_top, 0, 2)

        self.btn_tempo_plus = QPushButton("+")
        self.btn_tempo_plus.setFixedSize(24, 24)
        self.btn_tempo_plus.clicked.connect(self.increaseTempo)
        self.btn_tempo_plus.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: 1px solid #333333; } QPushButton:hover { background-color: #333333; }")
        keytempo_layout.addWidget(self.btn_tempo_plus, 0, 3)

        self.btn_go = QPushButton("GO")
        self.btn_go.setFixedSize(40, 24)
        self.btn_go.setObjectName("goButton")
        self.btn_go.setStyleSheet("#goButton { background-color: #333333; border: 1px solid #333333; color: #FFFFFF; } #goButton:hover { background-color: #1a1a1a; }")
        self.btn_go.clicked.connect(self.onGoButtonPressed)
        keytempo_layout.addWidget(self.btn_go, 0, 4)

        self.lbl_current_key = QLabel("0")
        self.lbl_current_key.setAlignment(Qt.AlignCenter)
        self.lbl_current_key.setStyleSheet("QLabel { color: #FFFFFF; background-color: #101010; padding: 0 4px; }")
        keytempo_layout.addWidget(self.lbl_current_key, 1, 0)

        self.btn_key_minus = QPushButton("-")
        self.btn_key_minus.setFixedSize(24, 24)
        self.btn_key_minus.clicked.connect(self.decreaseKey)
        self.btn_key_minus.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: 1px solid #333333; } QPushButton:hover { background-color: #333333; }")
        keytempo_layout.addWidget(self.btn_key_minus, 1, 1)

        self.lbl_current_tempo = QLabel("0%")
        self.lbl_current_tempo.setAlignment(Qt.AlignCenter)
        self.lbl_current_tempo.setStyleSheet("QLabel { color: #FFFFFF; background-color: #101010; padding: 0 4px; }")
        keytempo_layout.addWidget(self.lbl_current_tempo, 1, 2)

        self.btn_tempo_minus = QPushButton("-")
        self.btn_tempo_minus.setFixedSize(24, 24)
        self.btn_tempo_minus.clicked.connect(self.decreaseTempo)
        self.btn_tempo_minus.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: 1px solid #333333; } QPushButton:hover { background-color: #333333; }")
        keytempo_layout.addWidget(self.btn_tempo_minus, 1, 3)

        self.lbl_status = ClickableLabel()
        self.lbl_status.setFixedSize(40, 24)
        self.lbl_status.setAlignment(Qt.AlignCenter)
        self.lbl_status.setStyleSheet("background-color: #1a1a1a; border: 1px solid #333333; color: #FFFFFF; padding: 0px;")
        gear_icon = QIcon(resource_path("settings.png"))
        gear_pix = gear_icon.pixmap(16, 16)
        self.lbl_status.setPixmap(gear_pix)
        self.lbl_status.setToolTip("Settings")
        self.lbl_status.clicked.connect(self.openTempSettingsDialog)
        keytempo_layout.addWidget(self.lbl_status, 1, 4)

        cb_layout.addWidget(right_widget)
        control_bar.setLayout(cb_layout)
        bf_layout.addWidget(control_bar)

        self.queue_row = CategoryRow(
            'Queue (0 songs - 0:00)', 
            has_plus=False, 
            collapsible=False, 
            fixed_height=48  
        )
        self.queue_row.setObjectName("QueueRow")
        self.queue_row.label.setAlignment(Qt.AlignCenter)
        self.queue_row.setAttribute(Qt.WA_StyledBackground, True)

        self.queue_row.setStyleSheet("""
        QWidget#QueueRow {
            background-color: #202020;
            border: 1px solid #333333;
            border-radius: 6px;
            margin-left: 6px;    /* Horizontal margin so it’s inset */
            margin-right: 6px;
        }
        QWidget#QueueRow:hover {
            background-color: #302f2f;
        }
        QWidget#QueueRow:pressed {
            background-color: #444444;
        }
        """)

        self.queue_row.setContextMenuPolicy(Qt.CustomContextMenu)
        self.queue_row.customContextMenuRequested.connect(self.onQueueRowContextMenu)

        self.queue_row.rowClicked.connect(self.onQueueRowClicked)

        bf_layout.addWidget(self.queue_row)

        self.categories_list = DragDropListWidget(self)
        self.categories_list.setStyleSheet(
            "QListWidget { background-color: #181818; color: #FFFFFF; font-size: 14px; outline: none; }"
            "QListWidget::item { padding: 4px 8px; }"
            "QListWidget::item:hover { background-color: #302f2f; border-radius: 4px; }"
            "QListWidget::item:selected { background-color: #252424; border: none; }"
        )
        self.categories_list.verticalScrollBar().setStyleSheet(
            "QScrollBar:vertical { background: #181818; width: 4px; margin: 0; }"
            "QScrollBar::handle:vertical { background: #3C3C3C; min-height: 20px; width: 2px; }"
            "QScrollBar::handle:vertical:hover { background: #8f8f8f; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { background: none; border: none; height: 0; }"
            "QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background: none; margin: 0; }"
        )
        self.categories_list.setSpacing(2)
        self.categories_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.categories_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.categories_list.customContextMenuRequested.connect(self.onCategoriesContextMenu)

        self.categories_scroll = QScrollArea()
        self.categories_scroll.setWidgetResizable(True)
        self.categories_scroll.setWidget(self.categories_list)
        self.categories_scroll.setStyleSheet("QScrollArea { background-color: #181818; }")

        bf_layout.addWidget(self.categories_scroll)

        self.bottom_fixed.setLayout(bf_layout)
        self.left_splitter.addWidget(self.bottom_fixed)
        self.left_splitter.setStretchFactor(0, 0)
        self.left_splitter.setStretchFactor(1, 1)

        lp_layout.addWidget(self.left_splitter)
        self.horizontal_splitter.addWidget(self.left_panel)

        self.right_panel = QWidget()
        rp_layout = QVBoxLayout(self.right_panel)
        rp_layout.setContentsMargins(0, 0, 0, 0)
        rp_layout.setSpacing(0)

        search_hbox = QHBoxLayout()
        search_hbox.setContentsMargins(5, 0, 8, 0)
        search_hbox.setSpacing(4)

        self.btn_clear_search = QPushButton("Clear Filter")
        self.btn_clear_search.setFixedWidth(100)
        self.btn_clear_search.clicked.connect(self.clearSearchFields)
        self.btn_clear_search.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: 1px solid #333333; padding: 4px 6px; min-height: 28px; } QPushButton:hover { background-color: #333333; }")
        search_hbox.addWidget(self.btn_clear_search)

        self.song_search_line = SearchLineEdit()
        self.song_search_line.setPlaceholderText("Song...")
        self.song_search_line.setStyleSheet("QLineEdit { background-color: #181818; color: #FFFFFF; border: 1px solid #444444; padding: 4px; min-height: 28px; }")

        self.artist_search_line = SearchLineEdit()
        self.artist_search_line.setPlaceholderText("Artist...")
        self.artist_search_line.setStyleSheet("QLineEdit { background-color: #181818; color: #FFFFFF; border: 1px solid #444444; padding: 4px; min-height: 28px; }")

        if self.settings.value("searchRequiresEnter", True, type=bool):
            self.song_search_line.enterPressed.connect(self.updateFilter)
            self.artist_search_line.enterPressed.connect(self.updateFilter)
        else:
            self.song_search_line.textChanged.connect(self.updateFilter)
            self.artist_search_line.textChanged.connect(self.updateFilter)

        search_hbox.addWidget(self.song_search_line)
        search_hbox.addWidget(self.artist_search_line)

        self.btn_youtube = QPushButton("Send to YouTube")
        self.btn_youtube.setFixedWidth(120)
        self.btn_youtube.clicked.connect(self.searchYouTube)
        self.btn_youtube.setStyleSheet("QPushButton { background-color: #242424; color: #FFFFFF; border: 1px solid #333333; padding: 4px 6px; min-height: 28px; } QPushButton:hover { background-color: #333333; }")
        search_hbox.addWidget(self.btn_youtube)

        rp_layout.addLayout(search_hbox)

        self.songs_model = SongsTableModel([], show_key_tempo=True, view_mode="default")
        self.songs_model.history_mode = False
        self.proxy_model = TwoFieldFilterProxyModel()
        self.proxy_model.setSourceModel(self.songs_model)
        self.proxy_model.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.proxy_model.setFilterKeyColumn(-1)

        self.table_view = DragDropTableView(self)
        self.table_view.setItemDelegate(LeftAlignDelegate(self.table_view))
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_view.setModel(self.proxy_model)
        self.table_view.setSortingEnabled(True)
        self.proxy_model.sort(1, Qt.AscendingOrder)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        self.table_view.setColumnWidth(0, 250)
        self.table_view.setColumnWidth(1, 150)
        self.table_view.setColumnWidth(2, 60)
        self.table_view.setColumnWidth(3, 50)
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self.onTableContextMenu)
        self.table_view.doubleClicked.connect(self.onSongDoubleClick)
        self.table_view.horizontalHeader().sectionClicked.connect(self.onHeaderClicked)
        self.table_view.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.table_view.setContentsMargins(0, 0, 0, 0)

        self.table_view.setStyleSheet(
            "QTableView { background-color: #181818; color: #FFFFFF; gridline-color: #333333; font-size: 14px; }"
            "QTableView::item:selected { background-color: #252424; color: #FFFFFF; border: none; }"
            "QHeaderView::section { background-color: #242424; color: #8f8f8f; }"
            "QTableView::viewport { background-color: #181818; }"
            "QTableCornerButton::section { background-color: #242424; border: 1px solid #333333; }"
            "QTableView::verticalHeader { background-color: #242424; }"
            "QTableView::verticalHeader::section { background-color: #242424; color: #8f8f8f; }"
        )
        self.table_view.verticalHeader().setStyleSheet("""
            QHeaderView {
                background-color: #181818;
            }
            QHeaderView::section {
                background-color: #242424;
                border: 1px solid #333333;
                border-top: 0;
                color: #8f8f8f;
            }
        """)
        self.table_view.verticalHeader().setDefaultAlignment(Qt.AlignCenter)

        self.table_view.verticalScrollBar().setStyleSheet(
            "QScrollBar:vertical { background: #181818; width: 20px; margin: 0; }"
            "QScrollBar::handle:vertical { background: #3C3C3C; min-height: 20px; }"
            "QScrollBar::handle:vertical:hover { background: #8f8f8f; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { background: none; border: none; height: 0; }"
            "QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background: none; margin: 0; }"
        )
        self.table_view.verticalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.table_view.verticalHeader().setDefaultSectionSize(29)
        delegate = LeftAlignDelegate(self.table_view)
        for col in range(4):
            self.table_view.setItemDelegateForColumn(col, delegate)

        table_container = QWidget()
        table_container_layout = QHBoxLayout(table_container)
        table_container_layout.setContentsMargins(0, 0, 0, 0)
        table_container_layout.setSpacing(0)

        self.alphabet_panel = AutoWidthScrollArea()
        self.alphabet_panel.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.alphabet_panel.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.alphabet_panel.setWidgetResizable(True)
        self.alphabet_panel.setStyleSheet(
            "QScrollArea { background-color: #181818; }"
            "QScrollBar:vertical { background: #181818; width: 4px; margin: 0; }"
            "QScrollBar::handle:vertical { background: #3C3C3C; min-height: 20px; width: 2px; }"
            "QScrollBar::handle:vertical:hover { background: #8f8f8f; width: 2px; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { background: none; border: none; height: 0; }"
            "QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background: none; margin: 0; }"
        )

        self.alphabet_inner_widget = QWidget()
        self.alphabet_inner_widget.setFixedWidth(40)
        self.alphabet_inner_layout = QVBoxLayout(self.alphabet_inner_widget)
        self.alphabet_inner_layout.setContentsMargins(0, 0, 0, 0)
        self.alphabet_inner_layout.setSpacing(2)
        self.alphabet_inner_widget.setStyleSheet("background-color: #181818;")

        self.alphabet_panel.setWidget(self.alphabet_inner_widget)

        table_container_layout.addWidget(self.alphabet_panel)
        table_container_layout.addWidget(self.table_view)

        rp_layout.addWidget(table_container)
        self.right_panel.setLayout(rp_layout)
        self.horizontal_splitter.addWidget(self.right_panel)

        self.horizontal_splitter.setStyleSheet("QSplitter::handle { background-color: #1e1e1e; }")
        self.left_splitter.setStyleSheet("QSplitter::handle { background-color: #1e1e1e; }")

        self.buildCategories()
        self.hideHistorySubitems()

        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "LibraryCategory":
                self.categories_list.setCurrentItem(item)
                self.onCategoryClicked(item)
                QTimer.singleShot(200, lambda: self.alphabet_panel.resizeEvent(QResizeEvent(self.alphabet_panel.size(), self.alphabet_panel.size())))
                break

        QTimer.singleShot(200, lambda: self.alphabet_panel.resizeEvent(QResizeEvent(self.alphabet_panel.size(), self.alphabet_panel.size())))

        self.updateTableViewMode()
        self.table_view.selectionModel().selectionChanged.connect(self.onTableSelectionChanged)
        self.table_view.clicked.connect(lambda index: self.updateKeyTempoLabels(self.getSelectedTrack()))

        self.setStyleSheet("QMainWindow { background-color: #101010; margin: 0; padding: 0; }")
    def onQueueRowContextMenu(self, pos):
        """Right-click on the pinned Queue row to shuffle or clear."""
        menu = QMenu(self)

        act_shuffle = menu.addAction("Shuffle Queue")
        act_clear = menu.addAction("Clear Queue")

        chosen = menu.exec(self.queue_row.mapToGlobal(pos))
        if chosen == act_shuffle:
            self.shuffleQueue()
        elif chosen == act_clear:
            self.current_queue.clear()
            self.current_play_index = -1
            self.video_player.stop()
            self.audio_player_preset.stop()
            if self.current_view_mode == "queue":
                self.songs_model.setSongs(self.current_queue)
            self.updateQueueRowText()

    def updateAlphabetPanel(self, libraryName=None):
        while self.alphabet_inner_layout.count():
            item = self.alphabet_inner_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self.current_alphabet_button = None
        import sqlite3
        letters = []
        if self.current_view_mode in ('history', 'list'):
            songs = self.songs_model.songs()
            s = set()
            for si in songs:
                if si.artist and si.artist.strip():
                    s.add(si.artist[0].upper())
            letters = sorted(list(s))
        else:
            conn = sqlite3.connect("library.db")
            c = conn.cursor()
            if libraryName is None:
                c.execute("SELECT DISTINCT UPPER(SUBSTR(artist,1,1)) FROM songs ORDER BY UPPER(SUBSTR(artist,1,1))")
                letters = [row[0] for row in c.fetchall() if row[0] and row[0].strip()]
                conn.close()
            else:
                c.execute("SELECT DISTINCT UPPER(SUBSTR(artist,1,1)) FROM songs WHERE lib_name=? ORDER BY UPPER(SUBSTR(artist,1,1))", (libraryName,))
                letters = [row[0] for row in c.fetchall() if row[0] and row[0].strip()]
                conn.close()
            letters.sort()
        self.alphabet_inner_layout.addStretch()
        for let in letters:
            btn = QPushButton(let)
            btn.setFixedSize(35, 35)
            btn.clicked.connect(lambda checked, b=btn, l=let, lib=libraryName: self.letterButtonClicked(b, lib, l))
            btn.setStyleSheet("QPushButton { background-color: #202020; color: #FFFFFF; border: none; } QPushButton:hover { background-color: #333333; }")
            self.alphabet_inner_layout.addWidget(btn)
    def letterButtonClicked(self, button, libraryName, letter):
        if hasattr(self, 'current_alphabet_button') and self.current_alphabet_button is not None:
            self.current_alphabet_button.setStyleSheet('QPushButton { background-color: #202020; color: #FFFFFF; border: none; } QPushButton:hover { background-color: #333333; }')
        self.current_alphabet_button = button
        button.setStyleSheet('QPushButton { background-color: #333333; color: #FFFFFF; border: none; }')
        if self.current_view_mode == 'library':
            self.setupLazyLibrary(libraryName, letter)
        elif self.current_view_mode == 'history' or self.current_view_mode == 'list':
            if letter == 'None':
                if self._backup_songs is not None:
                    self.songs_model.setSongs(self._backup_songs)
            else:
                filtered = []
                if self._backup_songs is not None:
                    for s in self._backup_songs:
                        if s.artist.upper().startswith(letter.upper()):
                            filtered.append(s)
                    self.songs_model.setSongs(filtered)

    def onLetterButtonClicked(self, libraryName, letter):
        self.setupLazyLibrary(libraryName, letter)

    def setupLazyLibrary(self, libName, letter_filter):
        self.current_view_mode = "library"
        lazy_model = LazyLibraryModel(self, "library.db", libName, letter_filter, chunk_size=200)
        self.table_view.setModel(lazy_model)
        self.table_view.setSortingEnabled(True)
        lazy_model.fetchMore(QModelIndex())
        self.table_view.verticalScrollBar().setValue(0)
        self.updateTableViewMode()

    def triggerFetchMoreIfNeeded(self):
        model = self.table_view.model()
        if not hasattr(model, "canFetchMore"):
            return
        sb = self.table_view.verticalScrollBar()
        if sb.value() >= sb.maximum():
            if model.canFetchMore(QModelIndex()):
                model.fetchMore(QModelIndex())

    def openTempSettingsDialog(self):
        dialog = TempSettingsDialog(self)
        dialog.exec()
    def onTableSelectionChanged(self, selected, deselected):
        song = self.getSelectedTrack()
        self.updateKeyTempoLabels(song)
    def updateTableViewMode(self):
        if self.current_view_mode == 'list':
            self.table_view.setModel(self.songs_model)
            self.table_view.setSortingEnabled(False)
            self.songs_model.editable_order = True
            self.table_view.setDragDropMode(QAbstractItemView.InternalMove)
            self.table_view.setDefaultDropAction(Qt.MoveAction)
            self.table_view.setDragDropOverwriteMode(False)
            self.alphabet_panel.setVisible(False)
        elif self.current_view_mode == 'history':
            self.table_view.setModel(self.songs_model)
            self.table_view.setSortingEnabled(True)
            self.songs_model.editable_order = False
            self.table_view.setDragDropMode(QAbstractItemView.DragOnly)
            self.table_view.sortByColumn(self.songs_model.columnCount()-1, Qt.DescendingOrder)
            self.alphabet_panel.setVisible(False)
        elif self.current_view_mode == 'queue':
            self.table_view.setModel(self.songs_model)
            self.table_view.setSortingEnabled(False)
            self.songs_model.editable_order = False
            self.table_view.setDragDropMode(QAbstractItemView.DragOnly)
            self.alphabet_panel.setVisible(False)
        else:
            if isinstance(self.table_view.model(), LazyLibraryModel):
                self.table_view.setSortingEnabled(False)
                self.table_view.sortByColumn(1, Qt.AscendingOrder)
                self.alphabet_panel.setVisible(True)
            else:
                self.table_view.setModel(self.proxy_model)
                self.table_view.setSortingEnabled(True)
                self.proxy_model.sort(1, Qt.AscendingOrder)
                self.alphabet_panel.setVisible(True)

    def getSelectedTrack(self):
        indexes = self.table_view.selectionModel().selectedRows()
        if indexes:
            model = self.table_view.model()
            if isinstance(model, LazyLibraryModel):
                return model.getSongItem(indexes[0].row())
            elif self.current_view_mode in ("list", "history", "queue"):
                return self.songs_model.getSongItem(indexes[0].row())
            elif model == self.proxy_model:
                source_index = self.proxy_model.mapToSource(indexes[0])
                return self.songs_model.getSongItem(source_index.row())
        return None

    def buildCategories(self):
        self.categories_list.clear()
        self.library_row = CategoryRow('Libraries', has_plus=True)
        item_lib = QListWidgetItem(self.categories_list)
        item_lib.setData(Qt.UserRole, 'LibraryCategory')
        item_lib.setBackground(QColor('#1c1c1c'))
        self.categories_list.setItemWidget(item_lib, self.library_row)
        item_lib.setSizeHint(self.library_row.sizeHint())
        self.library_category_item = item_lib
        for libname in self.library_map.keys():
            li = QListWidgetItem('         ' + libname)
            li.setData(Qt.UserRole, 'LibrarySub')
            self.categories_list.addItem(li)
        self.history_row = CategoryRow('History', has_plus=False, collapsible=True, toggle_callback=self.toggleHistoryExpansion)
        item_hist = QListWidgetItem(self.categories_list)
        item_hist.setData(Qt.UserRole, 'HistoryCategory')
        item_hist.setBackground(QColor('#1c1c1c'))
        self.categories_list.setItemWidget(item_hist, self.history_row)
        item_hist.setSizeHint(self.history_row.sizeHint())
        self.history_subitems = ['         Today', '         Yesterday', '         This Week', '         Last Week', '         This Month', '         Last Month', '         This Year', '         All Time']
        for sub in self.history_subitems:
            hi = QListWidgetItem(sub)
            hi.setData(Qt.UserRole, 'HistorySub')
            hi.setHidden(True)
            self.categories_list.addItem(hi)
        self.lists_row = CategoryRow('Lists', has_plus=True)
        item_lists = QListWidgetItem(self.categories_list)
        item_lists.setData(Qt.UserRole, 'ListsCategory')
        item_lists.setBackground(QColor('#1c1c1c'))
        self.categories_list.setItemWidget(item_lists, self.lists_row)
        item_lists.setSizeHint(self.lists_row.sizeHint())
        for list_name in self.user_lists:
            li = QListWidgetItem('         ' + list_name)
            li.setData(Qt.UserRole, 'ListSub')
            self.categories_list.addItem(li)
        self.categories_list.itemClicked.connect(self.onCategoryClicked)
        self.categories_list.itemDoubleClicked.connect(self.onCategoryDoubleClicked)
        self.categories_list.itemChanged.connect(self.onCategoryItemChanged)
        self.library_row.plusClicked.connect(self.addFolder)
        self.lists_row.plusClicked.connect(self.createNewList)
        QTimer.singleShot(0, lambda: self.categories_list.doItemsLayout())

    def hideHistorySubitems(self):
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "HistorySub":
                item.setHidden(True)
    def createNewList(self):
        lists_folder = Path("Lists")
        lists_folder.mkdir(exist_ok=True)
        default_name = f"{datetime.datetime.now().strftime('%Y-%m-%d')}.txt"
        default_path = str(lists_folder / default_name)
        file_path, _ = QFileDialog.getSaveFileName(self, "Create New List", default_path, "Text Files (*.txt)")
        if file_path:
            if not file_path.endswith(".txt"):
                file_path += ".txt"
            Path(file_path).touch()
            list_name = Path(file_path).stem
            self.user_lists[list_name] = file_path
            li = QListWidgetItem("         " + list_name)
            li.setData(Qt.UserRole, "ListSub")
            li.setFlags(li.flags() | Qt.ItemIsEditable)
            self.categories_list.addItem(li)
    def deleteListFile(self, list_name):
        fn = self.user_lists.get(list_name, "")
        if fn and os.path.exists(fn):
            os.remove(fn)
        if list_name in self.user_lists:
            self.user_lists.pop(list_name)
        for i in range(self.categories_list.count()):
            item = self.categories_list.item(i)
            if item.data(Qt.UserRole) == "ListSub" and item.text().strip() == list_name:
                self.categories_list.takeItem(i)
                break
    def saveListToFile(self, list_name, songs):
        old_file = self.user_lists.get(list_name, "")
        if not old_file:
            new_file = f"{list_name}.txt"
            self.user_lists[list_name] = new_file
            old_file = new_file

        with open(old_file, "w", encoding="utf-8") as f:
            for s in songs:
                lib = s.lib_name if s.lib_name else ""
                fn_only = os.path.basename(s.file_path)
                line = f"{lib}<<<{fn_only}<<<{s.key_change}<<<{s.tempo_change}<<<{s.duration_ms}\n"
                f.write(line)
    def loadListFromFile(self, list_name):
        items = []
        fn = self.user_lists.get(list_name, "")
        if not fn or not os.path.exists(fn):
            return items

        with open(fn, "r", encoding="utf-8") as f:
            lines = f.readlines()

        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            parts = ln.split("<<<")
            if len(parts) < 5:
                continue

            lib_name, file_only, key_s, tempo_s, dur_s = parts
            try:
                key_c = int(key_s)
            except:
                key_c = 0
            try:
                tempo_c = int(tempo_s)
            except:
                tempo_c = 0
            try:
                dms = int(dur_s)
            except:
                dms = 0

            full_path = file_only  
            if lib_name and lib_name in self.library_map:
                folder = self.library_map[lib_name]
                full_path = str(Path(folder) / file_only)

            extension = Path(full_path).suffix.casefold()
            artist, title = parse_filename_for_artist_song(Path(full_path).name)

            si = SongItem(full_path, extension, artist, title, dms)
            si.key_change = key_c
            si.tempo_change = tempo_c
            si.lib_name = lib_name

            if dms == 0:
                dur = self.getDurationWithFfprobe(full_path)
                si.duration_ms = dur
                si.duration_str = ms_to_mmss(dur)

            items.append(si)

        return items
    def showHistory(self, timeframe):
        lines = []
        if os.path.exists(HISTORY_LOG_FILE):
            with open(HISTORY_LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
        now = datetime.datetime.now()
        songs = []
        for ln in lines:
            ln = ln.strip()
            if not ln:
                continue
            parts = ln.split("<<<")
            if len(parts) < 6:
                continue
            dt_str = parts[0].strip()
            lib = parts[1].strip()
            fn_only = parts[2].strip()
            try:
                dtp = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            formatted_dt = dtp.strftime("%H:%M:%S %m-%d-%Y")
            if not self.filterHistoryByTimeframe(dtp, timeframe, now):
                continue
            try:
                keyc = int(parts[3].strip())
            except Exception:
                keyc = 0
            try:
                tempo = int(parts[4].strip())
            except Exception:
                tempo = 0
            try:
                duration = int(parts[5].strip())
            except Exception:
                duration = 0
            file_type = Path(fn_only).suffix.casefold()
            artist, title = parse_filename_for_artist_song(fn_only)
            full_path = fn_only
            if lib and lib in self.library_map and self.library_map[lib]:
                folder = self.library_map[lib]
                full_path = str(Path(folder) / fn_only)
            si = SongItem(full_path, file_type, artist, title, duration)
            si.lib_name = lib
            si.key_change = keyc
            si.tempo_change = tempo
            si.history_dt = formatted_dt
            songs.append(si)
        self.songs_model.history_mode = True
        self.songs_model.show_key_tempo = True
        self.songs_model.setSongs(songs)
        self._backup_songs = songs[:]
        self.current_view_mode = "history"
        self.current_list_name = None
        self.updateTableViewMode()

    def exitFullscreen(self):
        if self.video_widget.isFullScreen():
            self.video_widget.setFullScreen(False)

    def filterHistoryByTimeframe(self, dtp, timeframe, now):
        if timeframe == "Today":
            return dtp.date() == now.date()
        elif timeframe == "Yesterday":
            return dtp.date() == (now.date() - datetime.timedelta(days=1))
        elif timeframe == "This Week":
            sow = now - datetime.timedelta(days=now.weekday())
            return dtp.date() >= sow.date()
        elif timeframe == "Last Week":
            sow = now - datetime.timedelta(days=now.weekday())
            lw_start = sow - datetime.timedelta(days=7)
            lw_end = sow - datetime.timedelta(days=1)
            return lw_start.date() <= dtp.date() <= lw_end.date()
        elif timeframe == "This Month":
            return (dtp.year == now.year and dtp.month == now.month)
        elif timeframe == "Last Month":
            month = now.month - 1
            year = now.year
            if month < 1:
                month = 12
                year -= 1
            return (dtp.year == year and dtp.month == month)
        elif timeframe == "This Year":
            return dtp.year == now.year
        elif timeframe == "All Time":
            return True
        return False
    def logToHistory(self, song_item):
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lib = song_item.lib_name if song_item.lib_name else ""
        fn_only = os.path.basename(song_item.file_path)
        line = f"{now_str}<<<{lib}<<<{fn_only}<<<{song_item.key_change}<<<{song_item.tempo_change}<<<{song_item.duration_ms}\n"
        with open(HISTORY_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)

    def showQueue(self):
        self.songs_model.history_mode = False
        self.songs_model.show_key_tempo = True
        self.songs_model.setSongs(self.current_queue)
        self.current_view_mode = "queue"
        self.current_list_name = None
        self.updateTableViewMode()
        self.updateKeyTempoLabelsForCurrentSong()
    def updateQueueRowText(self):
        def effective_duration_ms(s):
            tempo_factor = 1.0 + (s.tempo_change * 0.05)
            if tempo_factor <= 0:
                tempo_factor = 1.0
            return int(s.duration_ms / tempo_factor)
        total_ms = sum(effective_duration_ms(s) for s in self.current_queue)
        if 0 <= self.current_play_index < len(self.current_queue):
            current_song = self.current_queue[self.current_play_index]
            duration = effective_duration_ms(current_song)
            pos = self.video_player.position()
            fraction = 0
            if current_song.duration_ms > 0:
                fraction = pos / current_song.duration_ms
            left_in_current = int(duration * (1 - fraction))
            total_ms = left_in_current + sum(effective_duration_ms(s) for s in self.current_queue[self.current_play_index+1:])
        dur_str = ms_to_mmss(total_ms)
        self.queue_row.setText(f"Queue ({len(self.current_queue)} songs - {dur_str})")

    def addToQueue(self, song_item):
        if song_item.key_change != 0 or song_item.tempo_change != 0:
            combined_shifted = song_item.get_combined_shifted_audio_path(self.temp_folder)
            if combined_shifted and not Path(combined_shifted).exists():
                key_factor = 2.0 ** (song_item.key_change / 12.0) if song_item.key_change != 0 else 1.0
                tempo_factor = 1.0 + (song_item.tempo_change * 0.05) if song_item.tempo_change != 0 else 1.0
                request = (song_item, key_factor, tempo_factor, "last")
                self.render_queue.append(request)
                self.updateWindowTitle()
                if not self.combined_shift_processing:
                    self.processNextRenderRequest()
                return
        self.current_queue.append(song_item)
        self.updateQueueRowText()
    def shuffleQueue(self):
        if self.current_play_index >= 0:
            upcoming = self.current_queue[self.current_play_index + 1:]
            random.shuffle(upcoming)
            self.current_queue = self.current_queue[:self.current_play_index + 1] + upcoming
        else:
            random.shuffle(self.current_queue)
        if self.current_view_mode == "queue":
            self.songs_model.setSongs(self.current_queue)
            self.songs_model.layoutChanged.emit()
    def removeFromQueue(self, items):
        for si in items:
            if si in self.current_queue:
                idx = self.current_queue.index(si)
                self.current_queue.remove(si)
                if idx == self.current_play_index:
                    self.video_player.stop()
                    self.audio_player_preset.stop()
                    self.current_play_index = -1
        self.updateQueueRowText()
        if self.current_view_mode == "queue":
            self.songs_model.setSongs(self.current_queue)
    def setupShortcuts(self):
        play_pause_act = QAction(self)
        play_pause_act.setShortcut(QKeySequence(Qt.Key_Space))
        play_pause_act.triggered.connect(self.playPause)
        play_pause_act.setShortcutContext(Qt.ApplicationShortcut)
        self.addAction(play_pause_act)
        
        back_act = QAction(self)
        back_act.setShortcut(QKeySequence(Qt.Key_Left))
        back_act.triggered.connect(self.seekBackward)
        back_act.setShortcutContext(Qt.ApplicationShortcut)
        self.addAction(back_act)
        
        fwd_act = QAction(self)
        fwd_act.setShortcut(QKeySequence(Qt.Key_Right))
        fwd_act.triggered.connect(self.seekForward)
        fwd_act.setShortcutContext(Qt.ApplicationShortcut)
        self.addAction(fwd_act)
        
        idle_next_act = QAction(self)
        idle_next_act.setShortcut(QKeySequence("Alt+6"))
        idle_next_act.triggered.connect(self.nextRandomIdle)
        idle_next_act.setShortcutContext(Qt.ApplicationShortcut)
        self.addAction(idle_next_act)
        
        idle_force_act = QAction(self)
        idle_force_act.setShortcut(QKeySequence("Alt+2"))
        idle_force_act.triggered.connect(self.forceIdleLoop)
        idle_force_act.setShortcutContext(Qt.ApplicationShortcut)
        self.addAction(idle_force_act)
        
        idle_resume_act = QAction(self)
        idle_resume_act.setShortcut(QKeySequence("Alt+4"))
        idle_resume_act.triggered.connect(self.resumeIdleTimer)
        idle_resume_act.setShortcutContext(Qt.ApplicationShortcut)
        self.addAction(idle_resume_act)
        
        settings_act = QAction(self)
        settings_act.setShortcut(QKeySequence("Alt+8"))
        settings_act.triggered.connect(self.openTempSettingsDialog)
        settings_act.setShortcutContext(Qt.ApplicationShortcut)
        self.addAction(settings_act)
        
        esc_act = QAction(self)
        esc_act.setShortcut(QKeySequence(Qt.Key_Escape))
        esc_act.triggered.connect(self.exitFullscreen)
        esc_act.setShortcutContext(Qt.ApplicationShortcut)
        self.addAction(esc_act)

    def nextRandomIdle(self):
        if not self.current_queue:
            self._onIdleChangeTimer()
    def forceIdleLoop(self):
        if not self.current_queue:
            self.idleChangeTimer.stop()
    def resumeIdleTimer(self):
        if not self.current_queue:
            self.idleChangeTimer.start(self.idleChangeInterval * 1000)
    def seekBackward(self):
        if not self.current_queue:
            return
        if self.video_player.playbackState() == QMediaPlayer.PlayingState:
            t = self.video_player.position()
            new_time = max(t - 5000, 0)
            self.video_player.setPosition(new_time)
            self.audio_player_preset.setPosition(new_time)
            self.updateSecondMonitorSource()
    def seekForward(self):
        if not self.current_queue:
            return
        if self.video_player.playbackState() == QMediaPlayer.PlayingState:
            t = self.video_player.position()
            new_time = t + 5000
            self.video_player.setPosition(new_time)
            self.audio_player_preset.setPosition(new_time)
            self.updateSecondMonitorSource()
    def playPause(self):
        if not self.current_queue:
            return
        if self.video_player.playbackState() == QMediaPlayer.PlayingState:
            self.video_player.pause()
            self.audio_player_preset.pause()
            self.btn_play_pause_left.setIcon(self.play_icon)
        else:
            if self.current_play_index < 0 and self.current_queue:
                self.current_play_index = 0
                self.loadSong(self.current_queue[self.current_play_index])
            self.video_player.play()
            self.audio_player_preset.play()
            self.btn_play_pause_left.setIcon(self.style().standardIcon(QStyle.SP_MediaPause))
        if self.second_window and self.second_window.isVisible():
            if self.video_player.playbackState() == QMediaPlayer.PlayingState:
                self.second_window.player.play()
            else:
                self.second_window.player.pause()

    def playNext(self):
        if not self.current_queue:
            return
        if 0 <= self.current_play_index < len(self.current_queue):
            current_song = self.current_queue.pop(self.current_play_index)
            self.video_player.stop()
            self.audio_player_preset.stop()
            self.logToHistory(current_song)
            self.current_play_index -= 1
        if not self.current_queue:
            self.current_play_index = -1
            self.btn_play_pause_left.setIcon(self.play_icon)
            self.updateQueueRowText()
            if self.current_view_mode == "queue":
                self.songs_model.setSongs(self.current_queue)
            self.lbl_current_key.setText("0")
            self.lbl_current_tempo.setText("0%")
            if self.second_window and self.second_window.isVisible():
                self.second_window.player.stop()
            self.loadIdleVideo()
            return
        self.current_play_index += 1
        if self.current_play_index >= len(self.current_queue):
            self.current_play_index = 0
        next_song = self.current_queue[self.current_play_index]
        if next_song.is_rendering:
            next_song.render_intent = None
            self.video_player.stop()
            self.audio_player_preset.stop()
            self.loadSong(SongItem(next_song.file_path, next_song.file_type, next_song.artist, next_song.title, next_song.duration_ms))
            self.video_player.play()
            self.audio_player_preset.play()
            self.btn_play_pause_left.setIcon(self.style().standardIcon(QStyle.SP_MediaPause))
            self.updateQueueRowText()
            if self.current_view_mode == "queue":
                self.songs_model.setSongs(self.current_queue)
            return
        self.loadSong(next_song)
        self.video_player.play()
        self.audio_player_preset.play()
        self.btn_play_pause_left.setIcon(self.style().standardIcon(QStyle.SP_MediaPause))
        self.updateQueueRowText()
        if self.current_view_mode == "queue":
            self.songs_model.setSongs(self.current_queue)

    def loadSong(self, song_item):
        self.video_player.stop()
        self.audio_player_preset.stop()
        self.video_player.setLoops(1)
        self.audio_player_preset.setLoops(1)
        from pathlib import Path
        self.video_overlay.setOverlayEnabled(False)
        if song_item.file_type.casefold() == ".cdg" and (song_item.key_change != 0 or song_item.tempo_change != 0):
            processed = song_item.get_combined_shifted_audio_path(self.temp_folder)
            if processed and Path(processed).exists():
                self.video_player.setSource(QUrl.fromLocalFile(song_item.file_path))
                self.audio_player_preset.setSource(QUrl.fromLocalFile(processed))
                if song_item.tempo_change != 0:
                    self.video_player.setPlaybackRate(1.0 + (song_item.tempo_change * 0.05))
                if song_item.duration_ms == 0:
                    song_item.duration_ms = self.getDurationWithFfprobe(processed)
                    if song_item.duration_ms == 0:
                        song_item.duration_ms = self.getDurationWithFfprobe(song_item.file_path)
                    song_item.duration_str = ms_to_mmss(song_item.duration_ms)
                return
        else:
            if song_item.key_change != 0 and song_item.tempo_change != 0:
                processed = song_item.get_combined_shifted_audio_path(self.temp_folder)
                if processed and Path(processed).exists():
                    self.video_player.setSource(QUrl.fromLocalFile(processed))
                    self.audio_player_preset.setSource(QUrl.fromLocalFile(processed))
                    return
            elif song_item.tempo_change != 0 and song_item.key_change == 0:
                if song_item.tempo_shifted_audio_path and Path(song_item.tempo_shifted_audio_path).exists():
                    self.video_player.setSource(QUrl.fromLocalFile(song_item.tempo_shifted_audio_path))
                    self.audio_player_preset.setSource(QUrl.fromLocalFile(song_item.tempo_shifted_audio_path))
                    return
            elif song_item.key_change != 0 and song_item.tempo_change == 0:
                if song_item.shifted_audio_path and Path(song_item.shifted_audio_path).exists():
                    self.video_player.setSource(QUrl.fromLocalFile(song_item.shifted_audio_path))
                    self.audio_player_preset.setSource(QUrl.fromLocalFile(song_item.shifted_audio_path))
                    return
        if song_item.duration_ms == 0:
            song_item.duration_ms = self.getDurationWithFfprobe(song_item.file_path)
            song_item.duration_str = ms_to_mmss(song_item.duration_ms)
        self.video_player.setSource(QUrl.fromLocalFile(song_item.file_path))
        self.video_player.setPosition(0)
        self.audio_player_preset.setSource(QUrl.fromLocalFile(song_item.audio_file_path))
        self.audio_player_preset.setPosition(0)
        self.video_player.setPlaybackRate(1.0)
        self.updateSecondMonitorSource()
        self.updateKeyTempoLabels(song_item)

    def reloadSongKeepingTime(self, song_item):
        was_playing = (self.video_player.playbackState() == QMediaPlayer.PlayingState)
        if song_item.file_type.casefold() == ".cdg":
            self.loadSong(song_item)
            self.video_player.setPosition(0)
            self.audio_player_preset.setPosition(0)
            if was_playing:
                self.video_player.play()
                self.audio_player_preset.play()
            else:
                self.video_player.pause()
                self.audio_player_preset.pause()
        else:
            curt = self.video_player.position()
            self.loadSong(song_item)
            self.video_player.setPosition(curt)
            self.audio_player_preset.setPosition(curt)
            if was_playing:
                self.video_player.play()
                self.audio_player_preset.play()
            else:
                self.video_player.pause()
                self.audio_player_preset.pause()
    def _cdgSeekStep2(self):
        if self.video_player.playbackState() == QMediaPlayer.PlayingState:
            self.video_player.setPosition(self._cdg_seek_final_position)
            self.audio_player_preset.setPosition(self._cdg_seek_final_position)
        else:
            self.video_player.setPosition(self._cdg_seek_final_position)
            self.audio_player_preset.setPosition(self._cdg_seek_final_position)
            self.video_player.pause()
            self.audio_player_preset.pause()
    def updateKeyTempoLabels(self, song_item):
        if not song_item:
            self.lbl_current_key.setText("0")
            self.lbl_current_tempo.setText("0%")
            return
        if song_item.key_change == 0:
            self.lbl_current_key.setText("0")
        else:
            self.lbl_current_key.setText(f"{'+' if song_item.key_change > 0 else ''}{song_item.key_change}")
        tempo_percent = song_item.tempo_change * 5
        if tempo_percent == 0:
            self.lbl_current_tempo.setText("0%")
        else:
            sign = "+" if tempo_percent > 0 else ""
            self.lbl_current_tempo.setText(f"{sign}{tempo_percent}%")
    def updateKeyTempoLabelsForCurrentSong(self):
        if 0 <= self.current_play_index < len(self.current_queue):
            si = self.current_queue[self.current_play_index]
            self.updateKeyTempoLabels(si)
        else:
            self.lbl_current_key.setText("0")
            self.lbl_current_tempo.setText("0%")
    def onGoButtonPressed(self):
        selected = self.getSelectedTrack()
        if not selected:
            return
        if selected.key_change == 0 and selected.tempo_change == 0:
            return
        key_factor = 2.0 ** (selected.key_change / 12.0) if selected.key_change != 0 else 1.0
        tempo_factor = 1.0 + (selected.tempo_change * 0.05) if selected.tempo_change != 0 else 1.0
        request = (selected, key_factor, tempo_factor, "add_to_queue")
        self.render_queue.append(request)
        self.updateWindowTitle()
        if not self.combined_shift_processing:
            self.processNextRenderRequest()
    def processNextRenderRequest(self):
        if self.render_queue:
            request = self.render_queue.pop(0)
            self.startCombinedRender(request)
        else:
            self.updateWindowTitle()
    def startCombinedRender(self, request):
        selected, key_factor, tempo_factor, intent = request
        self._processing_request = request
        self._current_rendering_item = selected
        selected.is_rendering = True
        self.combined_shift_processing = True
        self.btn_go.setEnabled(True)
        self.lbl_status.setText("~")
        self.combined_shift_thread = QThread()
        self.combined_shift_worker = CombinedShiftWorker(selected, key_factor, tempo_factor, self.temp_folder)
        self.combined_shift_worker.moveToThread(self.combined_shift_thread)
        self.combined_shift_worker.progress.connect(self.updateCombinedProgress)
        self.combined_shift_worker.finished.connect(self.onCombinedShiftFinished)
        self.combined_shift_worker.finished.connect(self.combined_shift_worker.deleteLater)
        self.combined_shift_thread.finished.connect(self.combined_shift_thread.deleteLater)
        self.combined_shift_thread.started.connect(self.combined_shift_worker.run)
        self.combined_shift_thread.start()

    def updateCombinedProgress(self, value):
        self.lbl_status.setText(str(value))
    def onCombinedShiftFinished(self, success, message):
        selected, _, _, intent = self._processing_request
        self._processing_request = None
        self.combined_shift_processing = False
        self.btn_go.setEnabled(True)
        gear_icon = QIcon(resource_path("settings.png"))
        gear_pix = gear_icon.pixmap(14, 14)
        if not self._current_rendering_item:
            self.lbl_status.setPixmap(gear_pix)
        else:
            selected = self._current_rendering_item
            self._current_rendering_item = None
            selected.is_rendering = False
            if success:
                if self.current_view_mode == "list" and self.current_list_name:
                    self.saveListToFile(self.current_list_name, self.songs_model.songs())
                if selected in self.current_queue:
                    self.current_queue.remove(selected)
                if self.current_play_index < 0:
                    self.current_queue.insert(0, selected)
                    self.playNext()
                else:
                    self.current_queue.append(selected)
                self.updateKeyTempoLabels(selected)
                self.lbl_status.setPixmap(gear_pix)
            else:
                QMessageBox.warning(self, "Combined Shift Error", message)
                self.lbl_status.setPixmap(gear_pix)
        if self.combined_shift_thread:
            self.combined_shift_thread.quit()
            self.combined_shift_thread.wait()
            self.combined_shift_thread = None
        self.processNextRenderRequest()
        self.updateWindowTitle()
    def updateWindowTitle(self):
        if self.render_queue or self.combined_shift_processing:
            count = len(self.render_queue) + (1 if self.combined_shift_processing else 0)
            self.setWindowTitle(f"Karaoke Player - Rendering: {count} to be added to queue")
        else:
            self.setWindowTitle("Karaoke Player")
    def updatePlayerUI(self):
        if self._user_seeking:
            return

        if self.current_queue and 0 <= self.current_play_index < len(self.current_queue):
            si = self.current_queue[self.current_play_index]
            if si.file_type.casefold() == ".cdg":
                pos = self.audio_player_preset.position()
                tot = si.duration_ms
                if tot > 0:
                    fraction = pos / tot
                    progress = int(fraction * 1000)
                    self.seek_slider.setValue(progress)
                    self.lbl_current_time.setText(ms_to_mmss(pos))
                    self.lbl_total_time.setText(ms_to_mmss(tot))
                    if pos >= si.outro_start_ms and si.silence_detection_done:
                        self.playNext()
            else:
                duration = self.video_player.duration()
                if duration > 0:
                    pos = self.video_player.position()
                    fraction = pos / duration
                    progress = int(fraction * 1000)
                    self.seek_slider.setValue(progress)
                    self.lbl_current_time.setText(ms_to_mmss(pos))
                    self.lbl_total_time.setText(ms_to_mmss(duration))
        else:
            self.seek_slider.setValue(0)
            self.lbl_current_time.setText("0:00")
            self.lbl_total_time.setText("0:00")

        self.updateQueueRowText()
        if self.current_queue:
            self.btn_play_pause_left.setEnabled(True)
            self.btn_next_left.setEnabled(True)
        else:
            self.btn_play_pause_left.setEnabled(False)
            self.btn_next_left.setEnabled(False)

    def syncPlayers(self):
        if self.video_player.playbackState() == QMediaPlayer.PlayingState:
            if 0 <= self.current_play_index < len(self.current_queue):
                si = self.current_queue[self.current_play_index]
                if si.file_type.casefold() != ".cdg":
                    vpos = self.video_player.position()
                    apos = self.audio_player_preset.position()
                    diff = abs(vpos - apos)
                    if diff > 300:
                        self.audio_player_preset.setPosition(vpos)
                else:
                    if si.tempo_change != 0:
                        pass
                    else:
                        tempo_factor = 1.0 + (si.tempo_change * 0.05)
                        if abs(self.video_player.playbackRate() - tempo_factor) > 0.0001:
                            self.video_player.setPlaybackRate(tempo_factor)
        if self.second_window and self.second_window.isVisible():
            if self.second_window.player.playbackState() == QMediaPlayer.PlayingState:
                s_pos = self.second_window.player.position()
                v_pos = self.video_player.position()
                diff = abs(s_pos - v_pos)
                if diff > 300:
                    self.second_window.player.setPosition(v_pos)
    def onSongDoubleClick(self, index):
        model = self.table_view.model()
        si = None
        if isinstance(model, LazyLibraryModel):
            si = model.getSongItem(index.row())
        elif model == self.proxy_model:
            source_index = self.proxy_model.mapToSource(index)
            si = self.songs_model.getSongItem(source_index.row())
        elif model == self.songs_model:
            si = self.songs_model.getSongItem(index.row())
        if not si:
            return
        if si.is_rendering:
            QMessageBox.information(self, "Rendering in Progress", "Please wait, the song is still rendering.")
            return
        if self.current_view_mode == "queue":
            self.playNow(si)
        elif self.current_view_mode in ("list", "history"):
            self.addToQueue(si)
            if self.current_play_index < 0 or self.video_player.playbackState() in (QMediaPlayer.StoppedState, QMediaPlayer.PausedState):
                self.playNext()
        else:
            self.addToQueue(si)
            if self.current_play_index < 0 or self.video_player.playbackState() == QMediaPlayer.StoppedState:
                self.playNext()

    def playNow(self, si):
        if si.is_rendering:
            QMessageBox.information(self, "Rendering in Progress", "Please wait, the song is still rendering.")
            return
        if 0 <= self.current_play_index < len(self.current_queue):
            old_si = self.current_queue.pop(self.current_play_index)
            self.video_player.stop()
            self.audio_player_preset.stop()
            self.logToHistory(old_si)
        if si in self.current_queue:
            self.current_queue.remove(si)
        self.current_queue.insert(0, si)
        self.current_play_index = -1
        self.playNext()
        self.updateQueueRowText()
    def onTableContextMenu(self, pos):
        idxs = self.table_view.selectionModel().selectedRows()
        if not idxs:
            return
        sis = []
        current_model = self.table_view.model()
        for i in idxs:
            if current_model == self.songs_model:
                si = self.songs_model.getSongItem(i.row())
            elif current_model == self.proxy_model:
                source_index = self.proxy_model.mapToSource(i)
                si = self.songs_model.getSongItem(source_index.row())
            elif hasattr(current_model, 'getSongItem'):
                si = current_model.getSongItem(i.row())
            else:
                si = None
            if si:
                sis.append(si)
        if not sis:
            return
        menu = QMenu(self)
        if self.current_view_mode == "list":
            act_remove_from_list = menu.addAction("Delete song from list")
            act_add_to_queue = menu.addAction("Add to Queue")
            act_play_now = menu.addAction("Play NOW")
            act_add_another_list = menu.addMenu("Add to list:")
            for ln in self.user_lists:
                act_add_another_list.addAction(ln)
            act_search_for_artist = menu.addAction("Search for this artist")
            act_search_in_libraries_for_artist = menu.addAction("Search libraries for this artist")
            chosen = menu.exec(self.table_view.mapToGlobal(pos))
            if chosen == act_remove_from_list:
                changed = False
                for s in sis:
                    if s in self.songs_model._songs:
                        self.songs_model._songs.remove(s)
                        changed = True
                if changed and self.current_list_name:
                    self.saveListToFile(self.current_list_name, self.songs_model.songs())
                    self.songs_model.layoutChanged.emit()
                return
            if chosen == act_add_to_queue:
                for s in sis:
                    self.addToQueue(s)
                return
            if chosen == act_play_now:
                self.playNow(sis[0])
                return
            if chosen in act_add_another_list.actions():
                list_name = chosen.text()
                existing = self.loadListFromFile(list_name)
                for s in sis:
                    if not any(x.file_path == s.file_path for x in existing):
                        existing.append(s)
                self.saveListToFile(list_name, existing)
                return
            if chosen == act_search_for_artist:
                if sis:
                    artist = sis[0].artist
                    if self.current_view_mode == "library":
                        self.artist_search_line.setText(artist)
                        self.updateFilter()
                    elif self.current_view_mode == "list":
                        self.artist_search_line.setText(artist)
                        self.doUpdateFilter()
                    elif self.current_view_mode == "history":
                        self.artist_search_line.setText(artist)
                        self.doUpdateFilter()
                return
            if chosen == act_search_in_libraries_for_artist:
                if sis:
                    self.categories_list.clearSelection()
                    self.categories_list.setCurrentItem(self.library_category_item)
                    self.library_category_item.setSelected(True)
                    widget = self.categories_list.itemWidget(self.library_category_item)
                    if widget:
                        widget.setStyleSheet("QWidget#CategoryRow { background-color: #252424; }")
                    self.onCategoryClicked(self.library_category_item)
                    self.clearSearchFields()
                    self.artist_search_line.setText(sis[0].artist)
                    self.setupLazyLibrary(None, None)
                    self.updateFilter()
                return
            return
        if self.current_view_mode == "queue":
            act_now = menu.addAction("Play NOW")
            act_next = menu.addAction("Play next")
            act_last = menu.addAction("Play last")
            act_remove = menu.addAction("Remove from queue")
            addList_menu = menu.addMenu("Add to list:")
            for ln in self.user_lists:
                addList_menu.addAction(ln)
            chosen = menu.exec(self.table_view.mapToGlobal(pos))
            if not chosen:
                return
            if chosen == act_now:
                self.playNow(sis[0])
                return
            if chosen == act_next:
                ip = self.current_play_index + 1
                if ip < 0:
                    ip = 0
                for s in reversed(sis):
                    if s in self.current_queue:
                        self.current_queue.remove(s)
                    self.current_queue.insert(ip, s)
                return
            if chosen == act_last:
                for s in sis:
                    if s in self.current_queue:
                        idx = self.current_queue.index(s)
                        if idx == self.current_play_index:
                            self.current_queue.remove(s)
                            self.current_queue.append(s)
                            self.current_play_index = min(self.current_play_index, len(self.current_queue)-1)
                        else:
                            self.current_queue.remove(s)
                            self.current_queue.append(s)
                    else:
                        self.current_queue.append(s)
                return
            if chosen == act_remove:
                self.removeFromQueue(sis)
                return
            if chosen in addList_menu.actions():
                list_name = chosen.text()
                existing = self.loadListFromFile(list_name)
                for s in sis:
                    if not any(x.file_path == s.file_path for x in existing):
                        existing.append(s)
                self.saveListToFile(list_name, existing)
                return
            return
        act_addQ = menu.addAction("Add to Queue")
        act_playNow = menu.addAction("Play NOW")
        addList_menu = menu.addMenu("Add to list")
        for ln in self.user_lists:
            addList_menu.addAction(ln)
        act_search = menu.addAction("Search for this artist")
        act_search_lib_artist = menu.addAction("Search libraries for this artist")
        act_openloc = menu.addAction("Open file location")
        chosen = menu.exec(self.table_view.mapToGlobal(pos))
        if not chosen:
            return
        if chosen == act_addQ:
            for s in sis:
                self.addToQueue(s)
            return
        if chosen == act_playNow:
            self.playNow(sis[0])
            return
        if chosen in addList_menu.actions():
            list_name = chosen.text()
            existing = self.loadListFromFile(list_name)
            for s in sis:
                if not any(x.file_path == s.file_path for x in existing):
                    existing.append(s)
            self.saveListToFile(list_name, existing)
            return
        if chosen == act_search:
            if sis:
                artist = sis[0].artist
                if self.current_view_mode == "library":
                    self.artist_search_line.setText(artist)
                    self.updateFilter()
                elif self.current_view_mode == "list":
                    self.artist_search_line.setText(artist)
                    self.doUpdateFilter()
                elif self.current_view_mode == "history":
                    self.artist_search_line.setText(artist)
                    self.doUpdateFilter()
            return
        if chosen == act_search_lib_artist:
            if sis:
                self.categories_list.setCurrentItem(self.library_category_item)
                self.onCategoryClicked(self.library_category_item)
                self.clearSearchFields()
                self.artist_search_line.setText(sis[0].artist)
                self.setupLazyLibrary(None, None)
                self.updateFilter()
            return
        if chosen == act_openloc:
            if sis:
                fp = sis[0].file_path
                if os.name == 'nt':
                    os.system('explorer /select,"' + fp + '"')
                else:
                    os.system('xdg-open "' + os.path.dirname(fp) + '"')

    def onCategoryItemChanged(self, item: QListWidgetItem):
        role = item.data(Qt.UserRole)
        if role == "ListSub":
            new_name = item.text().strip()
            if not new_name:
                QMessageBox.warning(self, "Invalid Name", "List name cannot be empty.")
                return
    def fetchDurations(self, items):
        for si in items:
            if si.duration_ms == 0:
                for lib_name, folder in self.library_map.items():
                    if si.file_path.startswith(folder):
                        rel_path = os.path.relpath(si.file_path, folder)
                        key = (lib_name, rel_path)
                        if key in self.library_data:
                            si.duration_ms = self.library_data[key]['duration_ms']
                            si.duration_str = ms_to_mmss(si.duration_ms)
                            break
    def clearSearchFields(self):
        if hasattr(self, 'current_alphabet_button') and self.current_alphabet_button is not None:
            self.current_alphabet_button.setStyleSheet("QPushButton { background-color: #202020; color: #FFFFFF; border: none; } QPushButton:hover { background-color: #333333; }")
            self.current_alphabet_button = None
        self.song_search_line.clear()
        self.artist_search_line.clear()
        current_model = self.table_view.model()
        if self.current_view_mode == 'library':
            if isinstance(current_model, LazyLibraryModel):
                if current_model.lib_name is None:
                    self.aggregated_grouping = True
                current_model.setSongFilter('')
                current_model.setArtistFilter('')
                current_model.setLetterFilter(None)
                current_model.resetLoad()
            elif current_model == self.proxy_model:
                self.proxy_model.setSongFilter('')
                self.proxy_model.setArtistFilter('')
        elif self.current_view_mode in ('list', 'history'):
            if self._backup_songs is not None:
                self.songs_model.setSongs(self._backup_songs)
        self.doUpdateFilter()

    def updateFilter(self):
        if self.settings.value('searchRequiresEnter', True, type=bool):
            self._search_timer.stop()
            self.doUpdateFilter()
        else:
            if self.current_view_mode == 'library':
                if isinstance(self.table_view.model(), LazyLibraryModel):
                    model = self.table_view.model()
                    model.setSongFilter(self.song_search_line.text())
                    model.setArtistFilter(self.artist_search_line.text())
                    model.setLetterFilter(None)
                    model.resetLoad()
            elif self.current_view_mode in ('queue',):
                pass
            else:
                self._search_timer.start()

    def search_library(self, song_text, artist_text):
        results = []
        song_lower = song_text.casefold()
        artist_lower = artist_text.casefold()
        conn = sqlite3.connect('library.db')
        c = conn.cursor()
        query = "SELECT lib_name, filename, extension, artist, title, duration_ms FROM songs WHERE title LIKE ? AND artist LIKE ?"
        c.execute(query, (f"%{song_lower}%", f"%{artist_lower}%"))
        rows = c.fetchall()
        conn.close()
        for row in rows:
            ln, fn, ext, artist, title, dms = row
            folder = self.library_map.get(ln, "")
            full_path = str(Path(folder) / fn)
            si = SongItem(full_path, ext, artist, title, dms)
            results.append(si)
        results.sort(key=lambda s: (s.artist.casefold(), s.title.casefold()))
        self.search_queue.put(results)
        self.search_results_ready.emit()

    def applySearchResults(self):
        try:
            results = self.search_queue.get_nowait()
            self.songs_model.setSongs(results)
            self.table_view.setModel(self.songs_model)
            self.table_view.setSortingEnabled(True)
            self.table_view.horizontalHeader().setSortIndicator(1, Qt.AscendingOrder)
            self.updateTableViewMode()
        except queue.Empty:
            pass
    def doUpdateFilter(self):
        song_text = self.song_search_line.text()
        artist_text = self.artist_search_line.text()
        current_model = self.table_view.model()
        if isinstance(current_model, LazyLibraryModel):
            current_model.setSongFilter(song_text)
            current_model.setArtistFilter(artist_text)
            current_model.resetLoad()
        elif current_model == self.proxy_model:
            self.proxy_model.setSongFilter(song_text)
            self.proxy_model.setArtistFilter(artist_text)
        else:
            if not self._backup_songs:
                return
            filtered = []
            st_lower = song_text.casefold()
            at_lower = artist_text.casefold()
            for s in self._backup_songs:
                if st_lower in s.title.casefold() and at_lower in s.artist.casefold():
                    filtered.append(s)
            self.songs_model.setSongs(filtered)

    def searchYouTube(self):
        keywords = (self.song_search_line.text() + " " + self.artist_search_line.text()).strip()
        if not keywords:
            QMessageBox.warning(self, "Empty Search", "Please enter keywords.")
            return
        query = f"Karaoke+{'+'.join(keywords.split())}"
        url = f"https://www.youtube.com/results?search_query={query}"
        webbrowser.open_new_tab(url)
    def toggleSecondScreenPopout(self):
        if self.btn_monitor_left.isChecked():
            if not self.second_window:
                self.second_window = SecondScreenWindow(self)
                self.second_window.player.errorOccurred.connect(self.onSecondMonitorError)
                self.second_window.closed.connect(self.onSecondWindowClosed)
            self.second_window.show()
            src = self.video_player.source()
            self.second_window.player.stop()
            self.second_window.player.setSource(src)
            try:
                self.second_window.player.setLoops(getattr(self.video_player, 'loops', lambda: QMediaPlayer.Loops.Infinite)())
            except Exception:
                pass
            self.second_window.player.setPosition(self.video_player.position())
            if self.video_player.playbackState() == QMediaPlayer.PlayingState:
                self.second_window.player.play()
            else:
                self.second_window.player.pause()
        else:
            if self.second_window:
                self.second_window.close()
    def onSecondMonitorError(self, error):
        if error != QMediaPlayer.NoError:
            em = self.second_window.player.errorString()
            QMessageBox.warning(self, "Second Monitor Error", em)
            self.btn_monitor_left.setChecked(False)
            self.second_window.close()
    def onSecondWindowClosed(self):
        self.btn_monitor_left.setChecked(False)
        if self.second_window:
            self.second_window.player.stop()
            self.second_window = None
    def onMediaStatusChanged(self, status):
        if status == QMediaPlayer.EndOfMedia:
            if not self.current_queue:
                self.video_player.stop()
                self.audio_player_preset.stop()
                self.loadIdleVideo()
                return
            else:
                self.playNext()
        else:
            self.updateSecondMonitorSource()
    def onPlaybackError(self, error):
        if error != QMediaPlayer.NoError:
            if not hasattr(self, '_error_shown') or not self._error_shown:
                self._error_shown = True
                em = self.video_player.errorString() if self.video_player.error() != QMediaPlayer.NoError else self.audio_player_preset.errorString()
                if not em:
                    em = "Could not open file"
                QMessageBox.warning(self, "Playback Error", em)
                self.playNext()
                QTimer.singleShot(1000, lambda: setattr(self, '_error_shown', False))

    def closeEvent(self, event):
        self.settings.setValue("windowGeometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
        h = self.horizontal_splitter.sizes()
        self.settings.setValue("hsplitterSizes", h)
        v = self.left_splitter.sizes()
        self.settings.setValue("vsplitterSizes", v)
        if self.second_window and self.second_window.isVisible():
            self.second_window.close()
        if hasattr(self, 'combined_shift_thread') and self.combined_shift_thread and self.combined_shift_thread.isRunning():
            self.combined_shift_worker.cancel()
            self.combined_shift_thread.quit()
            self.combined_shift_thread.wait()
        if hasattr(self, 'silence_detect_thread') and self.silence_detect_thread and self.silence_detect_thread.isRunning():
            if hasattr(self, 'silence_worker'):
                self.silence_worker.cancel()
            self.silence_detect_thread.quit()
            self.silence_detect_thread.wait()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        super().closeEvent(event)
    def toggleFullscreen(self):
        if self.isFullScreen():
            self.showNormal()
        else:
            screen = self.screen()
            if screen:
                self.move(screen.availableGeometry().topLeft())
            self.showFullScreen()

    def updateLibrarySongs(self, songs):
        if self.current_view_mode == "library" and not self.grouped_mode:
            self.songs_model.setSongs(songs)
            self.table_view.setModel(self.proxy_model)
            self.table_view.setSortingEnabled(True)
            self.proxy_model.sort(1, Qt.AscendingOrder)
            self.updateTableViewMode()
        elif self.current_view_mode == "history":
            self.songs_model.setSongs(songs)
            self.table_view.setModel(self.songs_model)
            self.table_view.setSortingEnabled(True)
            self.table_view.sortByColumn(self.songs_model.columnCount() - 1, Qt.DescendingOrder)
            self.updateTableViewMode()
        elif self.current_view_mode == "list":
            self.songs_model.setSongs(songs)
            self.table_view.setModel(self.songs_model)
            self.table_view.setSortingEnabled(False)
            self.updateTableViewMode()
        else:
            self.songs_model.setSongs(songs)
            self.table_view.setModel(self.proxy_model)
            self.table_view.setSortingEnabled(True)
            self.proxy_model.sort(1, Qt.AscendingOrder)
            self.updateTableViewMode()

    def loadLibraryPaths(self):
        self.library_map.clear()
        conn = sqlite3.connect("library.db")
        c = conn.cursor()
        c.execute("SELECT lib_name, paths, sort_index FROM libraries ORDER BY sort_index")
        rows = c.fetchall()
        for row in rows:
            lib_name, raw_paths, sort_idx = row
            if not raw_paths:
                self.library_map[lib_name] = ""
                continue
            possible_paths = [p.strip() for p in raw_paths.splitlines() if p.strip()]
            c2 = sqlite3.connect("library.db")
            cc2 = c2.cursor()
            cc2.execute("SELECT filename FROM songs WHERE lib_name=? LIMIT 5", (lib_name,))
            first_five = cc2.fetchall()
            c2.close()
            found_valid = False
            for candidate_path in possible_paths:
                if not candidate_path:
                    continue
                missing_count = 0
                for frow in first_five:
                    test_fn = frow[0]
                    test_full = Path(candidate_path) / test_fn
                    try:
                        exists = test_full.exists()
                    except Exception:
                        exists = False
                    if not exists:
                        missing_count += 1
                        if missing_count > 0:
                            break
                if missing_count == 0:
                    self.library_map[lib_name] = candidate_path
                    found_valid = True
                    break
            if not found_valid:
                self.library_map[lib_name] = ""
                if first_five:
                    text_files = ", ".join([f[0] for f in first_five])
                else:
                    text_files = "(No files found in DB)"
                msg = QMessageBox(self)
                msg.setWindowTitle("Library Not Found")
                msg.setText("Unable to find library files for: " + lib_name)
                msg.setInformativeText("Checked these directories:\n" + "\n".join(possible_paths) + "\n\nLooked for files:\n" + text_files)
                edit_btn = msg.addButton("Edit library", QMessageBox.ActionRole)
                ignore_btn = msg.addButton("Ignore", QMessageBox.ActionRole)
                msg.setDefaultButton(ignore_btn)
                msg.exec()
                if msg.clickedButton() == edit_btn:
                    self.showEditLibraryDialog(lib_name)

    def saveLibraryPaths(self):
        pass

    def loadUserLists(self):
        self.user_lists = {}
        lists_folder = Path("Lists")
        if not lists_folder.exists():
            return
        for list_file in lists_folder.glob("*.txt"):
            list_name = list_file.stem
            self.user_lists[list_name] = str(list_file)
    def onSeekPress(self):
        self._user_seeking = True
    def onSeekRelease(self):
        self._user_seeking = False
        pos = self.seek_slider.value()
        if 0 <= self.current_play_index < len(self.current_queue):
            si = self.current_queue[self.current_play_index]
            if si.file_type.casefold() == ".cdg":
                total_ms = si.duration_ms
                new_pos = int(pos / 1000 * total_ms)
                self.video_player.setPosition(new_pos)
                self.audio_player_preset.setPosition(new_pos)
            else:
                duration = self.video_player.duration()
                new_pos = int(pos / 1000 * duration)
                self.video_player.setPosition(new_pos)
                self.audio_player_preset.setPosition(new_pos)
        self.updateSecondMonitorSource()
    def onSeekMove(self, value):
        if 0 <= self.current_play_index < len(self.current_queue):
            si = self.current_queue[self.current_play_index]
            if si.file_type.casefold() == ".cdg":
                total_ms = si.duration_ms
                new_pos = int(value / 1000 * total_ms)
                self.lbl_current_time.setText(ms_to_mmss(new_pos))
            else:
                duration = self.video_player.duration()
                new_pos = int(value / 1000 * duration)
                self.lbl_current_time.setText(ms_to_mmss(new_pos))
    def keyPressEvent(self, event):
        super().keyPressEvent(event)
    def toggleFullscreen(self):
        if self.video_widget.isFullScreen():
            self.video_widget.setFullScreen(False)
        else:
            self.video_widget.setFullScreen(True)
    def closeEvent(self, event):
        self.settings.setValue("windowGeometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
        h = self.horizontal_splitter.sizes()
        self.settings.setValue("hsplitterSizes", h)
        v = self.left_splitter.sizes()
        self.settings.setValue("vsplitterSizes", v)
        if self.second_window and self.second_window.isVisible():
            self.second_window.close()
        if hasattr(self, 'combined_shift_thread') and self.combined_shift_thread and self.combined_shift_thread.isRunning():
            self.combined_shift_worker.cancel()
            self.combined_shift_thread.quit()
            self.combined_shift_thread.wait()
        if hasattr(self, 'silence_detect_thread') and self.silence_detect_thread and self.silence_detect_thread.isRunning():
            if hasattr(self, 'silence_worker'):
                self.silence_worker.cancel()
            self.silence_detect_thread.quit()
            self.silence_detect_thread.wait()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        super().closeEvent(event)
    def updateSecondMonitorSource(self):
        if not self.second_window or not self.second_window.isVisible():
            return
        if self.current_play_index < 0 or self.current_play_index >= len(self.current_queue):
            return
        si = self.current_queue[self.current_play_index]
        from pathlib import Path
        if si.file_type.casefold() == ".cdg":
            self.second_window.player.setSource(QUrl.fromLocalFile(si.file_path))
            self.second_window.current_source = si.file_path
        else:
            if si.key_change != 0 and si.tempo_change != 0:
                combined_shifted = si.get_combined_shifted_audio_path(self.temp_folder)
                if combined_shifted and Path(combined_shifted).exists():
                    audio_path = combined_shifted
                else:
                    return
            elif si.key_change != 0 and si.shifted_audio_path:
                p = Path(si.shifted_audio_path)
                if p.exists():
                    audio_path = str(p)
                else:
                    audio_path = si.audio_file_path
            elif si.tempo_change != 0 and si.tempo_shifted_audio_path:
                p = Path(si.tempo_shifted_audio_path)
                if p.exists():
                    audio_path = str(p)
                else:
                    audio_path = si.audio_file_path
            else:
                audio_path = si.audio_file_path
            if si.key_change == 0 and si.tempo_change == 0:
                si.shifted_audio_path = None
                si.tempo_shifted_audio_path = None
                si.key_tempo_shifted_audio_path = None
            if not hasattr(self.second_window, 'current_source'):
                self.second_window.current_source = ""
            if self.second_window.current_source != audio_path:
                self.second_window.player.setSource(QUrl.fromLocalFile(audio_path))
                self.second_window.current_source = audio_path
        self.second_window.player.setPosition(self.video_player.position())
        if self.video_player.playbackState() == QMediaPlayer.PlayingState:
            self.second_window.player.play()
        else:
            self.second_window.player.pause()

    def updateLibrarySongs(self, songs):
        self.songs_model.setSongs(songs)
        self.table_view.setModel(self.songs_model)
        self.table_view.horizontalHeader().setSortIndicator(1, Qt.AscendingOrder)
        self.table_view.setSortingEnabled(True)
        self.updateTableViewMode()
    def saveLibraryPaths(self):
        pass
    def loadUserLists(self):
        self.user_lists = {}
        lists_folder = Path("Lists")
        if not lists_folder.exists():
            return
        for list_file in lists_folder.glob("*.txt"):
            list_name = list_file.stem
            self.user_lists[list_name] = str(list_file)
    def onSeekPress(self):
        self._user_seeking = True
    def onSeekRelease(self):
        self._user_seeking = False
        pos = self.seek_slider.value()
        if 0 <= self.current_play_index < len(self.current_queue):
            si = self.current_queue[self.current_play_index]
            if si.file_type.casefold() == ".cdg":
                total_ms = si.duration_ms
                new_pos = int(pos / 1000 * total_ms)
                self.video_player.setPosition(new_pos)
                self.audio_player_preset.setPosition(new_pos)
            else:
                duration = self.video_player.duration()
                new_pos = int(pos / 1000 * duration)
                self.video_player.setPosition(new_pos)
                self.audio_player_preset.setPosition(new_pos)
        self.updateSecondMonitorSource()
    def onSeekMove(self, value):
        if 0 <= self.current_play_index < len(self.current_queue):
            si = self.current_queue[self.current_play_index]
            if si.file_type.casefold() == ".cdg":
                total_ms = si.duration_ms
                new_pos = int(value / 1000 * total_ms)
                self.lbl_current_time.setText(ms_to_mmss(new_pos))
            else:
                duration = self.video_player.duration()
                new_pos = int(value / 1000 * duration)
                self.lbl_current_time.setText(ms_to_mmss(new_pos))
    def keyPressEvent(self, event):
        super().keyPressEvent(event)
    def toggleFullscreen(self):
        if self.video_widget.isFullScreen():
            self.video_widget.setFullScreen(False)
        else:
            self.video_widget.setFullScreen(True)
    def closeEvent(self, event):
        self.settings.setValue("windowGeometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
        h = self.horizontal_splitter.sizes()
        self.settings.setValue("hsplitterSizes", h)
        v = self.left_splitter.sizes()
        self.settings.setValue("vsplitterSizes", v)
        if self.second_window and self.second_window.isVisible():
            self.second_window.close()
        if hasattr(self, 'combined_shift_thread') and self.combined_shift_thread and self.combined_shift_thread.isRunning():
            self.combined_shift_worker.cancel()
            self.combined_shift_thread.quit()
            self.combined_shift_thread.wait()
        if hasattr(self, 'silence_detect_thread') and self.silence_detect_thread and self.silence_detect_thread.isRunning():
            if hasattr(self, 'silence_worker'):
                self.silence_worker.cancel()
            self.silence_detect_thread.quit()
            self.silence_detect_thread.wait()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        super().closeEvent(event)
    def setQueueRowActive(self, active: bool):
        """Apply or remove a 'selected' style to the pinned Queue row."""
        if active:
            self.queue_row.setStyleSheet("""
                QWidget#QueueRow {
                    background-color: #252424; 
                    border: 1px solid #333333;
                    border-radius: 6px;
                    margin-left: 5px;
                    margin-right: 5px;
                }
                QWidget#QueueRow:hover {
                    background-color: #252424;
                }
                QWidget#QueueRow:pressed {
                    background-color: #333333;
                }
            """)
        else:
            self.queue_row.setStyleSheet("""
                QWidget#QueueRow {
                    background-color: #202020;
                    border: 1px solid #333333;
                    border-radius: 6px;
                    margin-left: 10px;
                    margin-right: 9px;
                }
                QWidget#QueueRow:hover {
                    background-color: #302f2f;
                }
                QWidget#QueueRow:pressed {
                    background-color: #444444;
                }
            """)

    def onQueueRowClicked(self):
        """When the pinned Queue row is clicked."""
        self.categories_list.clearSelection()

        self.setQueueRowActive(True)

        self.current_view_mode = 'queue'
        self.current_list_name = None
        self.songs_model.history_mode = False
        self.showQueue()

class AggregateLibraryLoaderRunnable(QRunnable):
    def __init__(self, karaokePlayer):
        super().__init__()
        self.karaokePlayer = karaokePlayer
    def run(self):
        master_list = []
        for lib in self.karaokePlayer.library_map.keys():
            lib_songs = self.karaokePlayer.db_fetch_library_songs(lib, sort_by_artist=True)
            master_list.extend(lib_songs)
        QTimer.singleShot(0, lambda: self.karaokePlayer._updateMasterList(master_list))
class AutoWidthScrollArea(QScrollArea):
    def resizeEvent(self, event):
        super().resizeEvent(event)

class LazyLibraryModel(QAbstractTableModel):
    def __init__(self, parent, db_path, lib_name, letter_filter=None, chunk_size=200):
        super().__init__(parent)
        self.parent_ref = parent
        self.db_path = db_path
        self.lib_name = lib_name
        self.letter_filter = letter_filter
        self.song_filter = ''
        self.artist_filter = ''
        self.chunk_size = chunk_size

        self.sort_column = 1
        self.sort_order = Qt.AscendingOrder
        self.user_sorted = False

        self.songs = []
        self.total_count = 0
        self.loaded_count = 0
        self.loadTotalCount()

    def setSongFilter(self, text):
        self.song_filter = text

    def setArtistFilter(self, text):
        self.artist_filter = text

    def setLetterFilter(self, letter):
        self.letter_filter = letter

    def resetLoad(self):
        self.songs = []
        self.loaded_count = 0
        self.loadTotalCount()
        self.beginResetModel()
        self.endResetModel()

    def loadTotalCount(self):
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        libs = []
        if self.lib_name is None:
            for k in self.parent_ref.library_map.keys():
                libs.append(k)
        else:
            libs.append(self.lib_name)

        placeholders = ','.join(['?'] * len(libs))
        query = 'SELECT COUNT(*) FROM songs WHERE lib_name IN (' + placeholders + ')'
        params = libs[:]

        if self.letter_filter:
            query += ' AND artist LIKE ?'
            params.append(self.letter_filter + '%')

        if self.artist_filter:
            query += ' AND artist LIKE ?'
            params.append('%' + self.artist_filter + '%')

        if self.song_filter:
            query += ' AND title LIKE ?'
            params.append('%' + self.song_filter + '%')

        c.execute(query, tuple(params))
        row = c.fetchone()
        self.total_count = row[0] if row else 0
        conn.close()

    def rowCount(self, parent=QModelIndex()):
        return self.loaded_count

    def columnCount(self, parent=QModelIndex()):
        return 5 if self.lib_name is None else 4

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if index.row() >= len(self.songs):
            return None
        if role in (Qt.DisplayRole, Qt.EditRole):
            song = self.songs[index.row()]
            col = index.column()
            if col == 0:
                return song.title
            elif col == 1:
                return song.artist
            elif col == 2:
                return ms_to_mmss(song.duration_ms)
            elif col == 3:
                return song.file_type.lstrip('.')
            elif col == 4:
                return song.lib_name  
        if role == Qt.TextAlignmentRole:
            return Qt.AlignLeft | Qt.AlignVCenter
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal:
            if role == Qt.DisplayRole:
                headers = ['Song', 'Artist', 'Duration', 'File Type', 'Library']
                if section < len(headers):
                    return headers[section]
            elif role == Qt.TextAlignmentRole:
                return Qt.AlignLeft | Qt.AlignVCenter
        return super().headerData(section, orientation, role)

    def canFetchMore(self, parent):
        if parent.isValid():
            return False
        return self.loaded_count < self.total_count

    def fetchMore(self, parent):
        if parent.isValid():
            return
        import sqlite3
        from pathlib import Path
        remaining = self.total_count - self.loaded_count
        to_fetch = min(self.chunk_size, remaining)
        if to_fetch <= 0:
            return
        start = self.loaded_count
        self.beginInsertRows(QModelIndex(), start, start + to_fetch - 1)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        if self.lib_name is None:
            libs = list(self.parent_ref.library_map.keys())
            placeholders = ','.join(['?'] * len(libs))
            query = "SELECT s.lib_name, s.filename, s.extension, s.artist, s.title, s.duration_ms FROM songs s JOIN libraries l ON s.lib_name = l.lib_name WHERE s.lib_name IN (" + placeholders + ")"
            params = libs[:]
            if self.letter_filter:
                query += " AND s.artist LIKE ?"
                params.append(self.letter_filter + '%')
            if self.artist_filter:
                query += " AND s.artist LIKE ?"
                params.append('%' + self.artist_filter + '%')
            if self.song_filter:
                query += " AND s.title LIKE ?"
                params.append('%' + self.song_filter + '%')
            if self.parent_ref.aggregated_grouping:
                base_order = "l.sort_index ASC, s.artist COLLATE NOCASE ASC, s.title COLLATE NOCASE ASC"
                query += " ORDER BY " + base_order
            else:
                if self.sort_column == 0:
                    col_name = "s.title"
                elif self.sort_column == 1:
                    col_name = "s.artist"
                elif self.sort_column == 2:
                    col_name = "s.duration_ms"
                elif self.sort_column == 3:
                    col_name = "s.extension"
                else:
                    col_name = "s.artist"
                dir_str = "ASC" if self.sort_order == Qt.AscendingOrder else "DESC"
                query += " ORDER BY " + col_name + " COLLATE NOCASE " + dir_str
            query += " LIMIT ? OFFSET ?"
            params.extend([to_fetch, start])
            c.execute(query, tuple(params))
            rows = c.fetchall()
            conn.close()
            for row in rows:
                ln, fn, ext, artist, title, dms = row
                folder = self.parent_ref.library_map.get(ln, '')
                full_path = str(Path(folder) / fn)
                si = SongItem(full_path, ext, artist, title, dms)
                si.lib_name = ln
                self.songs.append(si)
        else:
            query = "SELECT lib_name, filename, extension, artist, title, duration_ms FROM songs WHERE lib_name = ?"
            params = [self.lib_name]
            if self.letter_filter:
                query += " AND artist LIKE ?"
                params.append(self.letter_filter + '%')
            if self.artist_filter:
                query += " AND artist LIKE ?"
                params.append('%' + self.artist_filter + '%')
            if self.song_filter:
                query += " AND title LIKE ?"
                params.append('%' + self.song_filter + '%')
            if self.sort_column == 1:
                order_dir = "ASC" if self.sort_order == Qt.AscendingOrder else "DESC"
                query += " ORDER BY artist " + order_dir + ", title ASC"
            else:
                order_field = "title" if self.sort_column == 0 else "duration_ms" if self.sort_column == 2 else "extension"
                order_dir = "ASC" if self.sort_order == Qt.AscendingOrder else "DESC"
                query += " ORDER BY " + order_field + " " + order_dir
            query += " LIMIT ? OFFSET ?"
            params.extend([to_fetch, start])
            c.execute(query, tuple(params))
            rows = c.fetchall()
            conn.close()
            for row in rows:
                ln, fn, ext, artist, title, dms = row
                folder = self.parent_ref.library_map.get(ln, '')
                full_path = str(Path(folder) / fn)
                si = SongItem(full_path, ext, artist, title, dms)
                si.lib_name = ln
                self.songs.append(si)
        self.loaded_count += to_fetch
        self.endInsertRows()

    def getSongItem(self, row):
        if 0 <= row < len(self.songs):
            return self.songs[row]
        return None

    def sort(self, column, order=Qt.AscendingOrder):
        self.sort_column = column
        self.sort_order = order
        self.user_sorted = True
        self.resetLoad()

    def setSortColumn(self, column, order):
        self.sort_column = column
        self.sort_order = order
        self.resetLoad()
        self.fetchMore(QModelIndex())

class SecondScreenWindow(QMainWindow):
    closed = Signal()
    def __init__(self, main_app):
        super().__init__()
        self.main_app = main_app
        self.setWindowTitle("Monitor Window - Karaoke Player")
        self.resize(800,450)
        self.video_widget = CustomVideoWidget()
        self.setCentralWidget(self.video_widget)
        self.player = QMediaPlayer()
        self.audio_output = QAudioOutput()
        self.audio_output.setVolume(0.0)
        self.player.setAudioOutput(self.audio_output)
        self.player.setVideoOutput(self.video_widget)
        self.video_widget.doubleClicked.connect(self.toggleFullscreen)
        self.installEventFilter(self)

    def toggleFullscreen(self):
        """Replace old QVideoWidget.setFullScreen calls with toggling the entire QMainWindow."""
        if self.isFullScreen():
            self.showNormal()
        else:
            screen = self.screen()  
            if screen:
                self.move(screen.availableGeometry().topLeft())
            self.showFullScreen()

    def eventFilter(self, source, event):
        if event.type() == QEvent.KeyPress:
            mod = event.modifiers()
            key = event.key()
            if not self.main_app.current_queue and (key == Qt.Key_Left or key == Qt.Key_Right):
                return True
            if (mod & Qt.AltModifier) and key == Qt.Key_6:
                self.main_app.nextRandomIdle()
                return True
            elif (mod & Qt.AltModifier) and key == Qt.Key_2:
                self.main_app.forceIdleLoop()
                return True
            elif (mod & Qt.AltModifier) and key == Qt.Key_4:
                self.main_app.resumeIdleTimer()
                return True
            elif (mod & Qt.AltModifier) and key == Qt.Key_8:
                self.main_app.openTempSettingsDialog()
                return True
            elif key == Qt.Key_Space:
                if not self.main_app.current_queue:
                    return True
                self.playPause()
                return True
            elif key == Qt.Key_Left:
                self.seekBackward()
                return True
            elif key == Qt.Key_Right:
                self.seekForward()
                return True
        return super().eventFilter(source, event)

    def playPause(self):
        if not self.current_queue:
            return
        if self.current_play_index < 0:
            self.current_play_index = 0
            self.loadSong(self.current_queue[0])
            self.video_player.play()
            self.audio_player_preset.play()
            self.btn_play_pause_left.setIcon(self.style().standardIcon(QStyle.SP_MediaPause))
            if self.second_window and self.second_window.isVisible():
                self.second_window.player.play()
            return
        if self.video_player.playbackState() == QMediaPlayer.PlayingState:
            self.video_player.pause()
            self.audio_player_preset.pause()
            self.btn_play_pause_left.setIcon(self.play_icon)
            if self.second_window and self.second_window.isVisible():
                self.second_window.player.pause()
        else:
            self.video_player.play()
            self.audio_player_preset.play()
            self.btn_play_pause_left.setIcon(self.style().standardIcon(QStyle.SP_MediaPause))
            if self.second_window and self.second_window.isVisible():
                self.second_window.player.play()

    def seekBackward(self):
        if self.player.playbackState() == QMediaPlayer.PlayingState:
            pos = max(self.player.position() - 5000, 0)
            self.player.setPosition(pos)
            self.main_app.video_player.setPosition(pos)
            self.main_app.audio_player_preset.setPosition(pos)

    def seekForward(self):
        if self.player.playbackState() == QMediaPlayer.PlayingState:
            pos = self.player.position() + 5000
            self.player.setPosition(pos)
            self.main_app.video_player.setPosition(pos)
            self.main_app.audio_player_preset.setPosition(pos)

    def closeEvent(self, event):
        self.closed.emit()
        super().closeEvent(event)

def main():
    if getattr(sys, "frozen", False):
        application_path = os.path.dirname(sys.executable)
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(application_path)
    instance_server = check_single_instance("KaraokePlayerInstance")
    if instance_server is None:
        print("Another instance is already running.")
        sys.exit(0)
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(resource_path("ico.ico")))
    app.setStyleSheet("QDialog, QMessageBox, QFileDialog, QInputDialog, QProgressDialog { background-color: #111111; color: #FFFFFF; } QDialog *, QMessageBox *, QFileDialog *, QInputDialog *, QProgressDialog * { color: #FFFFFF; } QDialog QPushButton, QMessageBox QPushButton, QFileDialog QPushButton, QInputDialog QPushButton, QProgressDialog QPushButton { background-color: #2a2a2a; border: 1px solid #333333; padding: 4px; } QDialog QPushButton:hover, QMessageBox QPushButton:hover, QFileDialog QPushButton:hover, QInputDialog QPushButton:hover, QProgressDialog QPushButton:hover { background-color: #333333; } QDialog QLineEdit, QMessageBox QLineEdit, QFileDialog QLineEdit, QInputDialog QLineEdit, QProgressDialog QLineEdit { background-color: #222222; border: 1px solid #333333; padding: 2px; } QTextEdit, QPlainTextEdit { background-color: #222222; color: #FFFFFF; border: 1px solid #333333; } QComboBox { background-color: #222222; color: #FFFFFF; border: 1px solid #333333; } QComboBox QAbstractItemView { background-color: #222222; color: #FFFFFF; selection-background-color: #333333; } QToolTip { color: #FFFFFF; background-color: #111111; border: none; } QMenu { background-color: #181818; color: #FFFFFF; border: 1px solid #333333; } QMenu::item { padding: 6px 12px; } QMenu::item:selected { background-color: #2a2a2a; border: none; }")
    player = KaraokePlayer()
    hss = player.settings.value("hsplitterSizes", None)
    if hss:
        player.horizontal_splitter.setSizes([int(x) for x in hss])
    vss = player.settings.value("vsplitterSizes", None)
    if vss:
        player.left_splitter.setSizes([int(x) for x in vss])
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            p = Path(arg)
            if p.suffix.lower() == ".cdg":
                audio_p = p.with_suffix(".mp3")
                if not audio_p.exists():
                    print("Audio file not found for", p)
                    continue
            if p.exists() and p.suffix.lower() in SUPPORTED_FILE_EXTENSIONS:
                artist, title = parse_filename_for_artist_song(p.name)
                dur = top_level_get_duration(str(p))
                sitem = SongItem(str(p), p.suffix.lower(), artist, title, dur)
                player.addToQueue(sitem)
            else:
                print("File not found or unsupported:", arg)
        if player.current_queue:
            player.playNext()
    player.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    QApplication.setAttribute(Qt.AA_Use96Dpi)
    main()