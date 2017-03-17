from mpld3 import plugins
import matplotlib


class EasyLineTooltip(plugins.PluginBase):

    JAVASCRIPT = """
    mpld3.register_plugin("easylinetooltip", EasyLineTooltipPlugin);
    EasyLineTooltipPlugin.prototype = Object.create(mpld3.Plugin.prototype);
    EasyLineTooltipPlugin.prototype.constructor = EasyLineTooltipPlugin;

    """

    def __init__(self):
        pass
