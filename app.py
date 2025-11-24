import threading
import time
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit, join_room
from datetime import datetime
import json
from sqlalchemy import Table, Column, Integer, Float, DateTime, select, update, insert
import os # Добавлен для работы с путями к файлам JSON

app = Flask(__name__)
# ВАШЕ ПОДКЛЮЧЕНИЕ К БД:
DATABASE_URL = 'postgresql+psycopg2://neondb_owner:npg_XTVb48QSFkPz@ep-dark-silence-adah8o3x-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'a_very_secret_and_unique_key_for_socketio'

# --- НАСТРОЙКИ ПУЛА ДЛЯ POSTGRES ---
app.config['SQLALCHEMY_POOL_PRE_PING'] = True 
app.config['SQLALCHEMY_POOL_RECYCLE'] = 300 
app.config['SQLALCHEMY_POOL_TIMEOUT'] = 10
# ------------------------------------

db = SQLAlchemy(app)
socketio = SocketIO(app)

# ------------------------------------------------------------------
# ГЛОБАЛЬНЫЕ НАСТРОЙКИ
# ------------------------------------------------------------------
TOURNAMENTS = ["APhB", "NChB", "AMB"] 
LEAGUES = ['Senior', 'Junior']
ALL_COMBINATIONS = [(t, l) for t in TOURNAMENTS for l in LEAGUES]

# ------------------------------------------------------------------
# 1. ДАННЫЕ О КОМАНДАХ ИЗ JSON
# ------------------------------------------------------------------

def load_teams_from_json(tournament_id, league_level):
    """
    Загружает список команд, считывая соответствующий JSON-файл с диска.
    Файл должен называться: [tournament_id.lower()]_[league_level.lower()].json
    """
    key = f"{tournament_id.lower()}_{league_level.lower()}"
    # Используем os.path.join для совместимости путей
    filename = os.path.join(os.path.dirname(__file__), f"{key}.json")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            teams = json.load(f)
            if isinstance(teams, list):
                return teams
            else:
                print(f"Ошибка: Файл {filename} содержит не список.")
                return []
    except FileNotFoundError:
        print(f"ВНИМАНИЕ: Файл данных команды не найден: {filename}")
        return []
    except json.JSONDecodeError:
        print(f"Ошибка: Неверный формат JSON в файле: {filename}")
        return []

# ------------------------------------------------------------------
# 2. CORE SQLAlchemy: Динамическое создание таблиц
# ------------------------------------------------------------------

def get_score_table(tournament_id, league_level):
    """
    Возвращает объект Table для нужной комбинации турнира/лиги.
    Создает его, если он еще не в метаданных.
    """
    table_name = f'score_{tournament_id}_{league_level}'
    
    if table_name not in db.metadata.tables:
        # Создаем колонки СВЕЖИМИ для каждой таблицы
        SCORE_TABLE_COLUMNS = [
            Column('id', Integer, primary_key=True),
            Column('team_id', Integer, nullable=False), # ID команды из JSON
            Column('tour_number', Integer, nullable=False),
            Column('score', Float, nullable=False, default=0.0),
            Column('timestamp', DateTime, default=datetime.utcnow)
        ]

        # Создаем объект Table, но пока не создаем саму таблицу в БД
        Table(table_name, db.metadata, *SCORE_TABLE_COLUMNS)
        print(f"Таблица {table_name} зарегистрирована в метаданных.")
        
    return db.metadata.tables[table_name]


def ensure_tables_exist():
    """Создает все 6 таблиц в БД, если они не существуют."""
    print("Проверка и создание 6 таблиц Score...")
    for t_id, l_level in ALL_COMBINATIONS:
        get_score_table(t_id, l_level) # Регистрируем все 6 таблиц
        
    # Создаем все зарегистрированные таблицы в БД
    # Вызываем db.metadata.create_all(db.engine) через контекст app
    db.metadata.create_all(db.engine)
    print("Все 6 таблиц Score готовы.")
    
# ------------------------------------------------------------------
# 3. ЛОГИКА РАСЧЕТА РЕЙТИНГА И ВЕЩАНИЕ
# ------------------------------------------------------------------

def calculate_ranking(tournament_id, league_level):
    """Рассчитывает рейтинг, используя команды из JSON и баллы из динамической таблицы."""
    
    teams_json = load_teams_from_json(tournament_id, league_level)
    
    if not teams_json:
        # Если команды не загружены (например, нет JSON-файла), возвращаем пустой рейтинг
        return [] 
        
    score_table = get_score_table(tournament_id, league_level)

    team_ids = [team['id'] for team in teams_json]
    
    # 2. Эффективно загружаем все баллы
    with db.session.no_autoflush:
        stmt = select(score_table.c.team_id, score_table.c.tour_number, score_table.c.score).where(
            score_table.c.team_id.in_(team_ids)
        )
        result = db.session.execute(stmt).all()
        # db.session.remove() # Не требуется, если мы не используем транзакцию здесь

    # 3. Инициализируем карту для подсчета
    scores_map = {}
    for team_id in team_ids:
        scores_map[team_id] = {'tour_scores': {i: 0.0 for i in range(1, 6)}, 'total_score': 0.0}

    # 4. Заполняем карту баллами из БД
    for t_id, t_num, score_val in result:
        if t_id in scores_map and 1 <= t_num <= 5:
            scores_map[t_id]['tour_scores'][t_num] = score_val
            scores_map[t_id]['total_score'] += score_val

    # 5. Формируем финальный список рейтинга
    ranking_list = []
    
    for team_data in teams_json:
        team_id = team_data['id']
        scores = scores_map.get(team_id, {'tour_scores': {i: 0.0 for i in range(1, 6)}, 'total_score': 0.0})
        
        ranking_list.append({
            'team_name': team_data['name'],
            'total_score': scores['total_score'],
            'tour_scores': scores['tour_scores'],
            'team_id': team_id,
            'tournament_id': tournament_id,
            'league_level': league_level
        })
    
    # 6. Сортировка и присвоение места
    ranking_list.sort(key=lambda x: x['total_score'], reverse=True)
    
    # ПРИМЕЧАНИЕ: Если вам нужна вторичная сортировка (например, по алфавиту), 
    # добавьте ее здесь: key=lambda x: (x['total_score'], x['team_name']), reverse=(True, False)
    
    for i, team in enumerate(ranking_list):
        team['rank'] = i + 1
        
    return ranking_list


def broadcast_full_dashboard():
    """Собирает данные для всех 6 рейтингов и отправляет их всем клиентам (для фонового обновления)."""
    with app.app_context():
        dashboard_data = {}
        for tour_id, level in ALL_COMBINATIONS:
            key = f'{tour_id}_{level}'
            ranking = calculate_ranking(tour_id, level) 
            dashboard_data[key] = {
                'title': f'{tour_id} ({level})',
                'data': ranking,
                'tournament_id': tour_id,
                'league_level': level
            }
        
        # Отправляем ВСЕ данные дашборда по общему событию
        socketio.emit('dashboard_update', dashboard_data)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Обновление дашборда отправлено.")
        return dashboard_data

def background_update_rankings():
    """Фоновый поток, который отправляет обновления дашборда через Socket.IO."""
    while True:
        try:
            broadcast_full_dashboard()
        except Exception as e:
            print(f"Ошибка в фоновом потоке при обновлении: {e}")
            # Добавьте log.exception(e) для более детального лога
            
        time.sleep(10) # Пауза 10 секунд

# ------------------------------------------------------------------
# 4. МАРШРУТЫ И SOCKETIO
# ------------------------------------------------------------------

@socketio.on('connect_to_ranking')
def handle_connect_to_ranking(data):
    """Обрабатывает подключение клиента к конкретному каналу рейтинга (не используется на дашборде)."""
    tournament_id = data.get('tournament_id')
    league_level = data.get('league_level')
    
    if tournament_id in TOURNAMENTS and league_level in LEAGUES:
        channel_name = f'ranking_{tournament_id}_{league_level}'
        join_room(channel_name)
        
        print(f"Клиент присоединился к комнате: {channel_name}")
        
        # Отправляем данные только этому клиенту при подключении
        ranking_data = calculate_ranking(tournament_id, league_level)
        emit('ranking_update', ranking_data, room=request.sid)

@app.route('/')
def index():
    """Перенаправляет базовый URL на дашборд со всеми рейтингами."""
    return redirect(url_for('all_rankings_dashboard'))


@app.route('/ranking/<string:tournament_id>/<string:league_level>')
def show_ranking(tournament_id, league_level):
    """Показывает рейтинг для одного турнира/лиги."""
    # Для этого маршрута требуется отдельный HTML-шаблон (ranking_template.html)
    display_level = league_level.capitalize()
    title = f"{tournament_id}: {display_level} Лига"
    return render_template('ranking_template.html', 
                           title=title,
                           tournament_id=tournament_id, 
                           league_level=league_level)


@app.route('/all_rankings')
def all_rankings_dashboard():
    """Собирает данные для всех 6 рейтингов для дашборда и отображает HTML."""
    
    # Расчет данных для начального рендера HTML
    dashboard_data = {}
    with app.app_context(): # Убедимся, что контекст активен для работы с БД
        for tour_id, level in ALL_COMBINATIONS:
            key = f'{tour_id}_{level}'
            ranking = calculate_ranking(tour_id, level)
            dashboard_data[key] = {
                'title': f'{tour_id} ({level})',
                'data': ranking,
                'tournament_id': tour_id,
                'league_level': level
            }
            
    # Передаем данные, чтобы Jinja2 мог их встроить в HTML
    return render_template('all_rankings_dashboard.html', 
                           data=dashboard_data, 
                           tournaments=TOURNAMENTS,
                           leagues=LEAGUES)


@app.route('/get_teams/<string:tournament_id>/<string:league_level>')
def get_teams(tournament_id, league_level):
    """Возвращает команды для AJAX-запроса, загружая их из JSON."""
    
    teams = load_teams_from_json(tournament_id, league_level)
    return jsonify(teams)


@app.route('/jury_input', methods=['GET', 'POST'])
def jury_input():
    """Обработка ввода баллов жюри."""
    if request.method == 'POST':
        data = request.json
        team_id = data.get('team_id')
        tour_number = data.get('tour_number')
        score_value = data.get('score')
        
        # 1. Валидация
        if team_id is None or tour_number is None or score_value is None:
            return jsonify({"success": False, "message": "Не хватает обязательных полей"}), 400
        
        try:
            team_id = int(team_id)
            tour_number = int(tour_number)
            score_value = float(score_value)
        except (TypeError, ValueError):
            return jsonify({"success": False, "message": "Неверный формат чисел для ID/тура/балла"}), 400

        if not (1 <= tour_number <= 5):
            return jsonify({"success": False, "message": "Неверный номер тура"}), 400

        # 2. Находим турнир/лигу по ID команды (поиск по JSON-данным)
        found_team = None
        tour_broadcast = None
        level_broadcast = None
        
        for tour_id, level in ALL_COMBINATIONS:
            teams = load_teams_from_json(tour_id, level)
            for team in teams:
                if team['id'] == team_id:
                    found_team = team
                    tour_broadcast = tour_id
                    level_broadcast = level
                    break
            if found_team:
                break
                
        if not found_team:
            return jsonify({"success": False, "message": f"Команда с ID {team_id} не найдена в данных"}), 404
        
        score_table = get_score_table(tour_broadcast, level_broadcast)
        
        # 3. Обновление/Вставка
        with db.session.begin(): # Используем контекстный менеджер для транзакции
            check_stmt = select(score_table.c.id).where(
                score_table.c.team_id == team_id,
                score_table.c.tour_number == tour_number
            )
            # scalar_one_or_none - более безопасно, чем scalar()
            existing_record_id = db.session.execute(check_stmt).scalar_one_or_none()
            
            if existing_record_id:
                # Обновление (UPDATE)
                update_stmt = update(score_table).where(score_table.c.id == existing_record_id).values(
                    score=score_value,
                    timestamp=datetime.utcnow()
                )
                db.session.execute(update_stmt)
                message = f"Баллы для команды {found_team['name']} (Тур {tour_number}) обновлены!"
            else:
                # Вставка (INSERT)
                insert_stmt = insert(score_table).values(
                    team_id=team_id, 
                    tour_number=tour_number, 
                    score=score_value
                )
                db.session.execute(insert_stmt)
                message = f"Баллы для команды {found_team['name']} (Тур {tour_number}) добавлены!"
            
        db.session.remove() # Возвращаем соединение в пул

        # 4. Отправка обновления (вызываем вещание полного дашборда)
        broadcast_full_dashboard()
        
        return jsonify({"success": True, "message": message})
        
    # GET-запрос: Загрузка формы
    return render_template('jury_input.html')


# ------------------------------------------------------------------
# 5. ЗАПУСК ПРИЛОЖЕНИЯ
# ------------------------------------------------------------------

if __name__ == '__main__':
    # Очистка и запуск в основном процессе/потоке
    with app.app_context():
        # Создаем все 6 таблиц при старте, если они не существуют
        ensure_tables_exist()
        print("6 таблиц Score готовы.")

    # ЗАПУСКАЕМ ФОНОВЫЙ ПОТОК SocketIO
    update_thread = threading.Thread(target=background_update_rankings, daemon=True)
    update_thread.start()
    print("Сервер запущен. Фоновое SocketIO вещание начато.")
    
    socketio.run(app, debug=True)