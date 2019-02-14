"""
Definição dos códigos que serão rodados pelo Celery.

Background tasks do sistema AJNA-virasana
Gerenciados por celery_.sh
Aqui ficam as rotinas que serão chamadas periodicamente.

"""

from virasana.workers.tasks import celery, processa_bson, processa_carga, processa_predictions, processa_stats


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """Agenda tarefas que serão executadas com frequência fixa.

    Os tempos são em segundos
    """
    # Tempos "quebrados" para evitar simultaneidade
    # processa_xml será chamado por processa_bson
    sender.add_periodic_task(30 * 60.0, processa_bson.s())  # 30 min
    # sender.add_periodic_task(11 * 60.1, processa_xml.s())  # 11 min
    sender.add_periodic_task(61 * 60.0, processa_predictions.s())  # 61 min
    sender.add_periodic_task(6 * 3603.0, processa_carga.s())  # 6h
    # sender.add_periodic_task(12 * 3600.00, processa_stats.s())  # 12h


