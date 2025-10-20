from quotient import baidu as baidu_market
from quotient import em as em_market
from quotient import identify_exchange, StockExchange
from quotient import tecent as tecent_market


def _convert_df_to_json(df, period):
    result = {
        'format' : 'json',
        'period' : period,
        'success' : True
    }
    if hasattr(df, 'attrs'):
        if 'code' in df.attrs and df.attrs['code'] is not None:
            result['code'] = df.attrs['code']
        if 'name' in df.attrs and df.attrs['name'] is not None:
            result['name'] = df.attrs['name']

    if period in ['daily', 'weekly', 'monthly']:
        datetime_format = '%Y-%m-%d'
    else:
        datetime_format = '%Y-%m-%d %H:%M:%S'
    
    datetime_cols = df.select_dtypes(include=['datetime64[ns]']).columns
    if len(datetime_cols) > 0:
        df_processed = df.copy()
        for col in datetime_cols:
            df_processed[col] = df_processed[col].dt.strftime(datetime_format)
        result['data'] = df_processed.to_dict(orient='records')
    else:
        result['data'] = df.to_dict(orient='records')
    return result

def get_quotation(symbol_type, code, period, start_time, end_time, adjust):
    market_type = identify_exchange(code)

    if symbol_type == 'stock':
        if market_type in (StockExchange.A_SHARE_NORTH_TRADING_MARKET,
                           StockExchange.UNKNOWN, StockExchange.HONG_KONG_STOCK_EXCHANGE):
            if market_type == StockExchange.HONG_KONG_STOCK_EXCHANGE and period in ('daily', 'minute', 'monthly'):
                markets = [tecent_market, baidu_market]
            else:
                markets = [baidu_market, tecent_market]
        else:
            markets = [baidu_market, tecent_market]
    else:
        markets = [em_market]

    errors = []
    for m in markets:
        try:
            if m == em_market:
                df = em_market.stock.get_quotation(code, period, start_time, end_time, adjust)
            else:
                df = m.stock.get_quotation(code, period, start_time, end_time)

            if df is not None and not df.empty:
                if 'code' in df.columns and 'name' in df.columns:
                    if len(df) > 0:
                        code_value = df['code'].iloc[0]
                        name_value = df['name'].iloc[0]
                        
                        df.attrs['code'] = code_value
                        df.attrs['name'] = name_value
                        
                        df = df.drop(columns=['code', 'name'])
                return _convert_df_to_json(df, period)

        except Exception as e:
            errors.append(str(e))
            continue

    return {'code' : code, 'period' : period, 'message': 'No data found.' if not errors else errors[-1], 'success' : False}