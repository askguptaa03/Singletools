import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np
import pandas as pd
import master_engine as m


def make_df(n=120):
    x = np.linspace(1.0, 1.5, n) + np.sin(np.arange(n)/4)*0.002
    return pd.DataFrame({
        'open': x-0.001, 'high': x+0.003, 'low': x-0.003,
        'close': x, 'volume': 1000,
    }, index=pd.date_range('2026-01-01', periods=n, freq='min', tz='UTC'))


def test_catalog_and_modes():
    assert len(m.STRATEGIES) >= 10
    assert set(m.QUALITY_MODES) == {'strict','balanced','opportunity'}
    assert set(m.ANALYSIS_MODES) == {'13-factor','selected-strategy','adaptive'}


def test_two_confirmations_are_required():
    df = make_df()
    r = m.analyze(df, {'price':float(df.close.iloc[-1]),'adx':30,'rsi':55,
                        'ema_9':float(df.close.iloc[-1]),'ema_21':float(df.close.iloc[-2])},
                   mode='selected-strategy', strategy='range_extreme_reversal')
    if r['signal'] in ('BUY','SELL'):
        assert r['confirmations']['mandatory'] == [True, True]


def test_30s_requires_real_cadence():
    df = make_df()
    r = m.analyze(df, {}, timeframe='30s', mode='adaptive')
    assert r['signal'] == 'WAIT'
    assert '30-second' in r.get('wait_reason','')


def test_13_factor_mode_does_not_create_master_signal():
    df = make_df()
    r = m.analyze(df, {}, mode='13-factor')
    assert r['signal'] == 'WAIT'
    assert r['confidence_score'] is None
