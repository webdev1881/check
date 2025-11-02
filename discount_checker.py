import asyncio
import aiohttp
import ssl
import pandas as pd
import random
from typing import List, Dict, Tuple
from dataclasses import dataclass
import logging
from pathlib import Path


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('discount_checker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Config:
    BASE_URL = "https://89.105.216.114"
    USERNAME = "Yulia"
    PASSWORD = "SY1804$@"
    
    BATCH_SIZE = 100
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
    
    EXCEL_FILE = "data.xlsx"  # Имя Excel файла в корне приложения


@dataclass
class RuleSet:
    """Набор правил для одной строки"""
    article: str  # Артикул из столбца C
    price: float  # Цена из столбца I
    
    # Уровни
    level_0: float  # Случайное число от 0 до K (не включая)
    level_1: float  # Случайное число от K (включая) до P (не включая)
    level_2: float  # Случайное число выше на 150-300% от P (включая)
    
    # Правила
    rule_0: float  # level_0 * price
    rule_1: float  # (price - L) * level_1
    rule_1_1: float  # Q * K
    rule_2: float  # (price - P) * level_2
    rule_2_1: float  # Q * P
    
    # Исходные данные для справки
    k_value: float
    l_value: float
    p_value: float
    q_value: float


class ExcelParser:
    """Парсер Excel файла"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        
    def parse(self) -> List[RuleSet]:
        """Парсит Excel и создает наборы правил для каждой строки"""
        try:
            # Читаем Excel файл
            df = pd.read_excel(self.file_path)
            logger.info(f"Excel файл загружен: {len(df)} строк")
            
            rule_sets = []
            
            for index, row in df.iterrows():
                # Проверяем, что столбец C не пустой
                if pd.isna(row.iloc[2]) or str(row.iloc[2]).strip() == '':
                    continue
                
                try:
                    # Извлекаем значения из столбцов
                    article = str(row.iloc[2]).strip()  # Столбец C
                    price = float(row.iloc[8])  # Столбец I
                    k_value = float(row.iloc[10])  # Столбец K
                    l_value = float(row.iloc[11])  # Столбец L
                    p_value = float(row.iloc[15])  # Столбец P
                    q_value = float(row.iloc[16])  # Столбец Q
                    
                    # Создаем уровни (предварительные расчеты)
                    level_0 = round(random.uniform(0, k_value), 2)
                    level_1 = round(random.uniform(k_value, p_value), 2)
                    level_2 = round(p_value * random.uniform(1.5, 3.0), 2)
                    
                    # Применяем правила
                    rule_0 = round(level_0 * price, 2)
                    rule_1 = round((price - l_value) * level_1, 2)
                    rule_1_1 = round((price - l_value) * k_value, 2)
                    rule_2 = round((price - q_value) * level_2, 2)
                    rule_2_1 = round((price - q_value) * p_value, 2)
                    
                    rule_set = RuleSet(
                        article=article,
                        price=price,
                        level_0=level_0,
                        level_1=level_1,
                        level_2=level_2,
                        rule_0=rule_0,
                        rule_1=rule_1,
                        rule_1_1=rule_1_1,
                        rule_2=rule_2,
                        rule_2_1=rule_2_1,
                        k_value=k_value,
                        l_value=l_value,
                        p_value=p_value,
                        q_value=q_value
                    )
                    
                    rule_sets.append(rule_set)
                    logger.debug(f"Создан набор правил для артикула {article}")
                    
                except (ValueError, IndexError) as e:
                    logger.warning(f"Ошибка обработки строки {index + 1}: {e}")
                    continue
            
            logger.info(f"Создано {len(rule_sets)} наборов правил")
            return rule_sets
            
        except Exception as e:
            logger.error(f"Ошибка чтения Excel файла: {e}")
            raise


class DiscountRulesAPI:
    """API клиент для работы с системой скидок"""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = None
        self.cookies = None
        
    async def __aenter__(self):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        self.session = aiohttp.ClientSession(connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            await asyncio.sleep(0.25)  # Даем время на закрытие
    
    async def login(self) -> bool:
        """Авторизация в системе"""
        url = f"{self.config.BASE_URL}/api/login"
        payload = {
            "username": self.config.USERNAME,
            "password": self.config.PASSWORD
        }
        
        try:
            async with self.session.post(url, json=payload) as response:
                if response.status == 200:
                    self.cookies = response.cookies
                    logger.info("Авторизация успешна")
                    return True
                else:
                    text = await response.text()
                    logger.error(f"Ошибка авторизации: {response.status} - {text}")
                    return False
        except Exception as e:
            logger.error(f"Ошибка при авторизации: {e}")
            return False
    
    async def get_discount_rules_page(self, offset: int = 0) -> Tuple[List[Dict], int]:
        """Получает одну страницу правил скидок"""
        url = f"{self.config.BASE_URL}/discountRule/list"
        
        payload = {
            "count": self.config.BATCH_SIZE,
            "filter": {},
            "offset": offset,
            "period": {},
            "sort": {
                "fields": [
                    {
                        "field": "name",
                        "asc": True
                    }
                ]
            }
        }
        
        headers = {
            'accept': '*/*',
            'content-type': 'application/json',
            'origin': self.config.BASE_URL,
            'referer': f"{self.config.BASE_URL}/",
            'user-agent': self.config.USER_AGENT
        }
        
        try:
            async with self.session.post(url, json=payload, headers=headers, cookies=self.cookies) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', []), data.get('count', 0)
                else:
                    text = await response.text()
                    logger.error(f"Ошибка получения данных: {response.status} - {text}")
                    return [], 0
        except Exception as e:
            logger.error(f"Ошибка при запросе данных: {e}")
            return [], 0
    
    async def get_all_discount_rules(self) -> List[Dict]:
        """Получает все правила скидок с учетом пагинации"""
        all_rules = []
        offset = 0
        
        while True:
            rules, total_count = await self.get_discount_rules_page(offset)
            
            if not rules:
                break
            
            all_rules.extend(rules)
            logger.info(f"Загружено {len(all_rules)} из {total_count} правил")
            
            if len(all_rules) >= total_count:
                break
            
            offset += self.config.BATCH_SIZE
        
        logger.info(f"Всего загружено {len(all_rules)} правил")
        return all_rules
    
    async def find_rules_by_articles(self, articles: List[str]) -> Dict[str, List[Dict]]:
        """Находит правила для списка артикулов"""
        all_rules = await self.get_all_discount_rules()
        
        # Группируем правила по артикулам
        rules_by_article = {article: [] for article in articles}
        
        for rule in all_rules:
            name = rule.get('name', '')
            
            # Проверяем структуру "Ахтирка_{Article}"
            if name.startswith('Ахтирка_'):
                article = name.split('Ахтирка_', 1)[1]
                
                if article in rules_by_article:
                    rules_by_article[article].append(rule)
        
        # Логируем результаты
        for article, rules in rules_by_article.items():
            if rules:
                logger.info(f"Для артикула {article} найдено {len(rules)} правил")
            else:
                logger.warning(f"Для артикула {article} не найдено правил")
        
        return rules_by_article
    
    async def test_discount_rule(self, article: str, quantity: float, price: float, terminal_id: int = 1541) -> Dict:
        """Тестирует правило скидки"""
        url = f"{self.config.BASE_URL}/discountRuleTester/process"
        
        payload = {
            "items": [
                {
                    "extSku": {
                        "id": article
                    },
                    "quantity": quantity,
                    "price": str(price),
                    "discount": 0,
                    "coupons": [],
                    "paidByPoints": None,
                    "appliedDiscountAmount": None,
                    "isFullTank": False,
                    "amount": round(quantity * price, 2)
                }
            ],
            "promoCodes": "",
            "cardCode": None,
            "clientId": None,
            "payFormType": 0,
            "terminalId": terminal_id,
            "date": "2025-11-01T16:46:39.609Z"
        }
        
        headers = {
            'accept': '*/*',
            'content-type': 'application/json',
            'origin': self.config.BASE_URL,
            'referer': f"{self.config.BASE_URL}/",
            'user-agent': self.config.USER_AGENT
        }
        
        try:
            async with self.session.post(url, json=payload, headers=headers, cookies=self.cookies) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'success': True,
                        'data': data,
                        'total_discount': data.get('data', {}).get('totalDiscountAmount', 0)
                    }
                else:
                    text = await response.text()
                    # Проверяем на ошибку БД - артикул не найден
                    if 'is not present in table' in text or 'ext_sku_group_id' in text:
                        return {
                            'success': False,
                            'error': 'Артикул не найден в системе',
                            'total_discount': 0
                        }
                    logger.error(f"Ошибка тестирования правила: {response.status} - {text}")
                    return {
                        'success': False,
                        'error': text[:200],  # Ограничиваем длину
                        'total_discount': 0
                    }
        except Exception as e:
            logger.error(f"Ошибка при тестировании правила: {e}")
            return {
                'success': False,
                'error': str(e)[:200],
                'total_discount': 0
            }


@dataclass
class ValidationCheck:
    """Результат проверки одного правила"""
    rule_name: str
    expected_discount: float
    actual_discount: float
    difference: float
    status: str  # 'OK', 'FAIL'
    error: str = None


class RulesValidator:
    """Валидатор правил"""
    
    def __init__(self, api: DiscountRulesAPI, terminal_id: int = 1541):
        self.api = api
        self.terminal_id = terminal_id
        self.results = []
    
    async def validate(self, rule_set: RuleSet, api_rules: List[Dict]) -> Dict:
        """Проверяет правила из API против расчетных правил"""
        validation_result = {
            'article': rule_set.article,
            'price': rule_set.price,
            'api_rules_count': len(api_rules),
            'checks': []
        }
        
        if not api_rules:
            validation_result['status'] = 'NO_API_RULES'
            validation_result['message'] = 'Правила не найдены в API'
            return validation_result
        
        logger.info(f"\n{'='*80}")
        logger.info(f"🔍 Проверка артикула: {rule_set.article}")
        logger.info(f"{'='*80}")
        
        # Проверяем каждое из 5 правил
        rules_to_check = [
            ('Правило 0', rule_set.level_0, rule_set.rule_0),
            ('Правило 1', rule_set.level_1, rule_set.rule_1),
            ('Правило 1-1', rule_set.k_value, rule_set.rule_1_1),
            ('Правило 2', rule_set.level_2, rule_set.rule_2),
            ('Правило 2-1', rule_set.p_value, rule_set.rule_2_1),
        ]
        
        for rule_name, quantity, price_with_discount in rules_to_check:
            # Цена без скидки
            price_without_discount = round(quantity * rule_set.price, 2)
            
            # Ожидаемая скидка = цена без скидки - цена со скидкой
            expected_discount = round(price_without_discount - price_with_discount, 2)
            
            logger.info(f"\n📋 {rule_name}:")
            logger.info(f"   Количество: {quantity}")
            logger.info(f"   Цена без скидки: {price_without_discount}")
            logger.info(f"   Цена со скидкой: {price_with_discount}")
            logger.info(f"   Ожидаемая скидка: {expected_discount}")
            
            # Тестируем через API
            result = await self.api.test_discount_rule(
                article=rule_set.article,
                quantity=quantity,
                price=rule_set.price,
                terminal_id=self.terminal_id
            )
            
            if result['success']:
                actual_discount = result['total_discount']
                difference = abs(expected_discount - actual_discount)
                
                # Определяем статус (допуск 0.01)
                status = 'OK' if difference <= 0.01 else 'FAIL'
                
                check = ValidationCheck(
                    rule_name=rule_name,
                    expected_discount=expected_discount,
                    actual_discount=actual_discount,
                    difference=difference,
                    status=status
                )
                
                # Красивый вывод
                if status == 'OK':
                    logger.info(f"   ✅ API скидка: {actual_discount} - СОВПАДАЕТ")
                else:
                    logger.warning(f"   ❌ API скидка: {actual_discount} - РАСХОЖДЕНИЕ {difference}")
                
            else:
                check = ValidationCheck(
                    rule_name=rule_name,
                    expected_discount=expected_discount,
                    actual_discount=0,
                    difference=expected_discount,
                    status='ERROR',
                    error=result.get('error', 'Unknown error')
                )
                logger.error(f"   ❌ Ошибка API: {check.error}")
            
            validation_result['checks'].append(check)
        
        # Подсчет статистики
        ok_count = sum(1 for c in validation_result['checks'] if c.status == 'OK')
        fail_count = sum(1 for c in validation_result['checks'] if c.status == 'FAIL')
        error_count = sum(1 for c in validation_result['checks'] if c.status == 'ERROR')
        
        validation_result['status'] = 'COMPLETED'
        validation_result['ok_count'] = ok_count
        validation_result['fail_count'] = fail_count
        validation_result['error_count'] = error_count
        validation_result['message'] = f'Проверено 5 правил: ✅ {ok_count} | ❌ {fail_count} | ⚠️ {error_count}'
        
        logger.info(f"\n📊 Итог: {validation_result['message']}")
        
        return validation_result
    
    def export_to_excel(self, filename: str = "validation_results.xlsx"):
        """Экспортирует результаты в Excel"""
        rows = []
        
        for result in self.results:
            article = result['article']
            price = result['price']
            
            for check in result['checks']:
                rows.append({
                    'Артикул': article,
                    'Цена': price,
                    'Правило': check.rule_name,
                    'Ожидаемая скидка': check.expected_discount,
                    'Фактическая скидка': check.actual_discount,
                    'Расхождение': check.difference,
                    'Статус': check.status,
                    'Ошибка': check.error if check.error else ''
                })
        
        df = pd.DataFrame(rows)
        df.to_excel(filename, index=False, engine='openpyxl')
        logger.info(f"\n💾 Результаты сохранены в {filename}")
        return filename


async def main():
    """Основная функция программы"""
    print("\n" + "="*80)
    print("🚀 ПРОГРАММА ПРОВЕРКИ ПРАВИЛ СКИДОК")
    print("="*80)
    
    logger.info("="*80)
    logger.info("Запуск программы проверки правил скидок")
    logger.info("="*80)
    
    # Проверяем наличие Excel файла
    excel_path = Path(Config.EXCEL_FILE)
    if not excel_path.exists():
        error_msg = f"❌ Excel файл не найден: {excel_path.absolute()}"
        print(error_msg)
        logger.error(error_msg)
        print("\n💡 Поместите файл 'data.xlsx' в корень приложения")
        return
    
    # Парсим Excel и создаем наборы правил
    print("\n📂 Шаг 1: Парсинг Excel файла...")
    logger.info("Шаг 1: Парсинг Excel файла")
    
    parser = ExcelParser(Config.EXCEL_FILE)
    rule_sets = parser.parse()
    
    if not rule_sets:
        error_msg = "❌ Не найдено ни одной строки для обработки"
        print(error_msg)
        logger.error(error_msg)
        return
    
    print(f"✅ Создано {len(rule_sets)} наборов правил")
    
    # Выводим примеры
    print("\n📋 Примеры созданных правил:")
    for i, rule_set in enumerate(rule_sets[:3], 1):
        print(f"\n   Артикул: {rule_set.article} | Цена: {rule_set.price}")
        print(f"   • Правило 0: {rule_set.rule_0}")
        print(f"   • Правило 1: {rule_set.rule_1}")
        print(f"   • Правило 1-1: {rule_set.rule_1_1}")
        print(f"   • Правило 2: {rule_set.rule_2}")
        print(f"   • Правило 2-1: {rule_set.rule_2_1}")
    
    if len(rule_sets) > 3:
        print(f"\n   ... и еще {len(rule_sets) - 3} артикулов")
    
    # Получаем список артикулов
    articles = [rs.article for rs in rule_sets]
    
    # Подключаемся к API
    print("\n" + "="*80)
    print("🌐 Шаг 2: Подключение к API")
    print("="*80)
    
    async with DiscountRulesAPI(Config()) as api:
        # Авторизация
        print("\n🔐 Авторизация...")
        if not await api.login():
            error_msg = "❌ Не удалось авторизоваться в системе"
            print(error_msg)
            logger.error(error_msg)
            return
        print("✅ Авторизация успешна")
        
        # Получаем правила
        print(f"\n🔍 Поиск правил для {len(articles)} артикулов...")
        rules_by_article = await api.find_rules_by_articles(articles)
        
        # Валидация
        print("\n" + "="*80)
        print("✓ Шаг 3: Проверка правил через API")
        print("="*80)
        
        validator = RulesValidator(api, terminal_id=1541)
        
        total_articles = len(rule_sets)
        for idx, rule_set in enumerate(rule_sets, 1):
            print(f"\n[{idx}/{total_articles}] Проверка артикула {rule_set.article}...")
            
            api_rules = rules_by_article.get(rule_set.article, [])
            result = await validator.validate(rule_set, api_rules)
            validator.results.append(result)
            
            if result['status'] == 'NO_API_RULES':
                print(f"   ⚠️  {result['message']}")
            else:
                print(f"   📊 {result['message']}")
    
    # Сохраняем в Excel
    print("\n" + "="*80)
    print("💾 Сохранение результатов...")
    print("="*80)
    
    excel_file = validator.export_to_excel("validation_results.xlsx")
    print(f"✅ Файл сохранен: {excel_file}")
    
    # Итоговая статистика
    print("\n" + "="*80)
    print("📊 ИТОГОВАЯ СТАТИСТИКА")
    print("="*80)
    
    total = len(validator.results)
    with_rules = sum(1 for r in validator.results if r['status'] == 'COMPLETED')
    without_rules = sum(1 for r in validator.results if r['status'] == 'NO_API_RULES')
    
    total_ok = sum(r.get('ok_count', 0) for r in validator.results)
    total_fail = sum(r.get('fail_count', 0) for r in validator.results)
    total_error = sum(r.get('error_count', 0) for r in validator.results)
    
    print(f"\n📦 Всего артикулов: {total}")
    print(f"✅ Проверено: {with_rules}")
    print(f"⚠️  Без правил в API: {without_rules}")
    print(f"\n🎯 Проверки правил:")
    print(f"   ✅ Успешно: {total_ok}")
    print(f"   ❌ Ошибки: {total_fail}")
    print(f"   ⚠️  API ошибки: {total_error}")
    
    print("\n" + "="*80)
    print("✅ ПРОГРАММА ЗАВЕРШЕНА")
    print("="*80)
    
    logger.info("\n" + "="*80)
    logger.info("ИТОГОВАЯ СТАТИСТИКА")
    logger.info("="*80)
    logger.info(f"Всего артикулов: {total}")
    logger.info(f"Проверено: {with_rules}")
    logger.info(f"Без правил: {without_rules}")
    logger.info(f"Успешных проверок: {total_ok}")
    logger.info(f"Ошибок: {total_fail}")
    logger.info(f"API ошибок: {total_error}")
    logger.info("="*80)
    logger.info("Программа завершена")
    logger.info("="*80)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nПрограмма прервана пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        
        
# source .venv/Scripts/activate
# python discount_checker.py