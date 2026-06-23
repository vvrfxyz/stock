"""Add pipeline_task_runs for scheduled_update observability

Revision ID: c8d9e0f1a2b3
Revises: b7c8d9e0f1a2
Create Date: 2026-06-23
"""
from alembic import op
import sqlalchemy as sa


revision = 'c8d9e0f1a2b3'
down_revision = 'b7c8d9e0f1a2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'pipeline_task_runs',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('run_id', sa.String(64), nullable=False, index=True,
                  comment='同一 scheduled_update 批次的所有 step 共享同一个 run_id'),
        sa.Column('task_name', sa.String(100), nullable=False, index=True),
        sa.Column('started_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('ended_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('exit_code', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(20), nullable=False, server_default='RUNNING',
                  comment='RUNNING / SUCCESS / FAILED / ERROR'),
        sa.Column('error_sample', sa.Text(), nullable=True,
                  comment='失败时的简短错误信息'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('ix_task_run_run_id_task', 'pipeline_task_runs', ['run_id', 'task_name'])


def downgrade() -> None:
    op.drop_index('ix_task_run_run_id_task', table_name='pipeline_task_runs')
    op.drop_table('pipeline_task_runs')
