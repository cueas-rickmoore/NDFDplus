
import os


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ForecastPathMethods:
    """ Methods for generating forecast directory and file paths.

    REQUIRED:
        1. must be included in a class derived from 
           atmosci.seasonal.factory.BaseProjectFactory
        2. subclass must also inherit from
           atmosci.seasonal.methods.paths.PathConstructionMethods
    """

    def forecastDownloadDir(self, fcast_source, fcast_date):
        # determine root directory of forecast tree
        if self.project.shared_forecast:
            fcast_dir = os.path.join(self.sharedRootDir(), 'forecast')
        else:
            fcast_dir = self.config.dirpaths.get('forecast', default=None)
            if fcast_dir is None:
                fcast_dir = os.path.join(self.projectRootDir(), 'forecast')
        # add subdirectory for forecast source
        download_dir = \
            os.path.join(fcast_dir, self.sourceToDirpath(fcast_source))
        # add subdirectory for forecast date
        download_dir = \
            os.path.join(download_dir, self.timeToDirpath(fcast_date))
        # make sure directory exists before return
        if not os.path.exists(download_dir): os.makedirs(download_dir)
        return download_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def forecastGribFilename(self, fcast_source, fcast_date, time_span,
                                   variable, **kwargs):
        template = self.getDownloadFileTemplate(fcast_source)
        template_args = dict(kwargs)
        template_args['date'] = self.timeToDirpath(fcast_date)
        template_args['source'] = self.sourceToFilepath(fcast_source)
        template_args['timespan'] = time_span
        template_args['variable'] = variable
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def forecastGribFilepath(self, fcast_source, fcast_date, time_span,
                                   variable, **kwargs):
        fcast_dir = self.forecastDownloadDir(fcast_source, fcast_date)
        filename = self.forecastGribFilename(fcast_source, fcast_date,
                                             time_span, variable, **kwargs)
        return os.path.join(fcast_dir, filename)

