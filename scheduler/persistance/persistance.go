package persistance

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Persistance struct {
	pool *pgxpool.Pool
}

var ErrSaldoInsuficiente = errors.New("saldo insuficiente en el contador")

func NewPersistance(ctx context.Context, connString string) (*Persistance, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("error parseando cadena de conexión: %w", err)
	}

	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("error creando pool de conexiones: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("no se pudo conectar a CockroachDB: %w", err)
	}

	return &Persistance{pool: pool}, nil
}

func (p *Persistance) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

func (p *Persistance) GetCounter(ctx context.Context) (int, error) {
	var valor int
	query := `SELECT valor FROM counter;`

	err := p.pool.QueryRow(ctx, query).Scan(&valor)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("contador no encontrado")
		}
		return 0, fmt.Errorf("error leyendo contador: %w", err)
	}

	return valor, nil
}

func (p *Persistance) DecrementCounter(ctx context.Context, id string, amount int) (int, error) {
	var nuevoValor int
	query := `
		UPDATE counter 
		SET valor = valor - $1 
		WHERE id = $2 AND valor >= $1 
		RETURNING valor;
	`

	err := p.pool.QueryRow(ctx, query, amount, id).Scan(&nuevoValor)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrSaldoInsuficiente
		}
		return 0, fmt.Errorf("error decrementando contador: %w", err)
	}

	return nuevoValor, nil
}

func (p *Persistance) SetCounter(ctx context.Context, id string, newValue int) error {
	query := `
		INSERT INTO counter (id, valor) 
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET valor = EXCLUDED.valor;
	`
	_, err := p.pool.Exec(ctx, query, id, newValue)
	if err != nil {
		return fmt.Errorf("error actualizando contador: %w", err)
	}
	return nil
}
