import click


class Context:
    def __init__(self, name, keys, seeds, working):
        self.name = name
        self.keys = keys
        self.seeds = seeds
        self.working = working


@click.group(invoke_without_command=True)
@click.option("-n", "--name", type=str, default="")
@click.option("-k", "--keys", type=str, multiple=True, default=[])
@click.option("-s", "--seeds", type=int, multiple=True, default=[])
@click.option("-w", "--working", type=str, default=".")
@click.pass_context
def cli(ctx, **kwargs):
    ctx.obj = Context(**kwargs)


@cli.command()
@click.pass_obj
def find_neighbors(obj, **kwargs):
    from .find_neighbors import FindNeighbors

    FindNeighbors(obj).run(**kwargs)


@cli.command()
@click.pass_obj
def compress_neighbors(obj, **kwargs):
    from .compress_neighbors import CompressNeighbors

    CompressNeighbors(obj).run(**kwargs)


@cli.command()
@click.pass_obj
def merge_neighbors(obj, **kwargs):
    from .merge_neighbors import MergeNeighbors

    MergeNeighbors(obj).run(**kwargs)


@cli.command()
@click.pass_obj
def analyze_clusters(obj, **kwargs):
    from .analyze_clusters import AnalyzeClusters

    AnalyzeClusters(obj).run(**kwargs)


@cli.command()
@click.pass_obj
def create_networks(obj, **kwargs):
    from .create_networks import CreateNetworks

    CreateNetworks(obj).run(**kwargs)


@cli.command()
@click.pass_obj
def analyze_measures(obj, **kwargs):
    from .analyze_measures import AnalyzeMeasures

    AnalyzeMeasures(obj).run(**kwargs)


@cli.command()
@click.pass_obj
def plot_clusters(obj, **kwargs):
    from .plot_clusters import PlotClusters

    PlotClusters(obj).run(**kwargs)


@cli.command()
@click.pass_obj
def plot_measures(obj, **kwargs):
    from .plot_measures import PlotMeasures

    PlotMeasures(obj).run(**kwargs)


@cli.command()
@click.pass_obj
def plot_neighbors(obj, **kwargs):
    from .plot_neighbors import PlotNeighbors

    PlotNeighbors(obj).run(**kwargs)


if __name__ == "__main__":
    cli()
