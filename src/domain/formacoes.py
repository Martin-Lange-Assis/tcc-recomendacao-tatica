FORMACOES = {
    '4-1-3-2': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 1, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_central', 'count': 1, 'posicoes': ['MC', 'MEI']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},
        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-1-4-1': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 1, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-2-3-1 (fechado)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 2, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_ofensivo', 'count': 3, 'posicoes': ['MEI', 'MC']},
        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],

    '4-2-3-1 (aberto)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 2, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-2-4': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-3-1-2': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 3, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-3-2-1': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 3, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_ofensivo', 'count': 2, 'posicoes': ['MEI', 'MC']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-3-3 (Em linha)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 3, 'posicoes': ['MC', 'VOL']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-3-3 (Conservador)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 1, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},
        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-3-3 (Defensivo)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 2, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_central', 'count': 1, 'posicoes': ['MC', 'VOL']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-3-3 (Ofensivo)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 2, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_ofensivo', 'count': 2, 'posicoes': ['MEI', 'MC']},
        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-2-2-2': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-1-2-1-2 (Aberto)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 1, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-1-2-1-2 (Fechado)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 1, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-4-2 (Em linha)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},
        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-4-2 (Conservador)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-4-1-1 (Meio-Campo)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-5-1 (Em Linha)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 3, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-5-1 (Ofensivo)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 1, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_ofensivo', 'count': 2, 'posicoes': ['MEI', 'MC']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '3-1-4-2': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'volante', 'count': 1, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '3-4-1-2': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '3-4-2-1': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'meia_ofensivo', 'count': 2, 'posicoes': ['MEI', 'MC']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '3-4-3 (Em Linha)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '3-5-2': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'volante', 'count': 2, 'posicoes': ['VOL', 'MC', 'MEI']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '5-2-1-2': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '5-2-3': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '5-3-2': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 1, 'posicoes': ['VOL', 'MC', 'MEI']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},

        {'slot': 'atacante', 'count': 2, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '5-4-1 (Em Linha)': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},

        {'slot': 'zagueiro', 'count': 3, 'posicoes': ['ZAG', 'VOL', 'LD', 'LE']},

        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'meia_central', 'count': 2, 'posicoes': ['MC', 'VOL', 'MEI']},
        {'slot': 'meia_dir', 'count': 1, 'posicoes': ['MD', 'PD', 'ATA']},
        {'slot': 'meia_esq', 'count': 1, 'posicoes': ['ME', 'PE', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-2-1-3': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 2, 'posicoes': ['VOL', 'MC', 'MEI']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},

        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']},
    ],
    '4-2-3-1': [
        {'slot': 'goleiro', 'count': 1, 'posicoes': ['GL']},
        {'slot': 'zagueiro', 'count': 2, 'posicoes': ['ZAG', 'VOL']},
        {'slot': 'lateral_dir', 'count': 1, 'posicoes': ['LD', 'ZAG', 'VOL', 'MD']},
        {'slot': 'lateral_esq', 'count': 1, 'posicoes': ['LE', 'ZAG', 'VOL', 'ME']},
        {'slot': 'volante', 'count': 2, 'posicoes': ['VOL', 'MC']},
        {'slot': 'meia_ofensivo', 'count': 1, 'posicoes': ['MEI', 'MC']},
        {'slot': 'ponta_dir', 'count': 1, 'posicoes': ['PD', 'MD', 'ATA']},
        {'slot': 'ponta_esq', 'count': 1, 'posicoes': ['PE', 'ME', 'ATA']},
        {'slot': 'atacante', 'count': 1, 'posicoes': ['ATA', 'PD', 'PE', 'MEI']}
    ]
}
