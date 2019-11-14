
import os
import datetime
import urllib

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class ReanalysisFactoryMethods:
    """ Methods for generating directory and file paths for downloaded
    RTMA and URMA reanalysis files.

    WARNING: Requires functionality contained in the base Seasonal Project
             Factory. So it MUST be included in a subclass based on the
             Seasonal Project Factory.
    """

    def completeInitialization(self, **kwargs):
        """ called by seaonal project factory base classes to allow
        subclasses to set 
        """
        analysis = kwargs.get('analysis', 'rtma')
        self.analysis = self.config.sources[analysis]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def timeOfLatestAnalysis(self):
        latest_time = datetime.datetime.utcnow()
        if latest_time.minute <= CACHE_SERVER_BUFFER_MIN:
            latest_time = (latest_time - datetime.timedelta(hours=1))
        return latest_time.replace(minute=0, second=0, microsecond=0)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # analysis directory & data file path
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def analysisDownloadDir(self, utc_hour):
        # determine root directory of analysis tree
        if self.project.shared_analysis:
            anal_dir = os.path.join(self.sharedRootDir(), 'analysis')
        else:
            anal_dir = self.config.dirpaths.get('analysis', default=None)
            if anal_dir is None:
                anal_dir = os.path.join(self.projectRootDir(), 'analysis')
        # add subdirectory for analysis source
        download_dir = \
            os.path.join(anal_dir, self.sourceToDirpath(self.ndfd_config))
        # add subdirectory for analysis date
        download_dir = \
            os.path.join(download_dir, self.timeToDirpath(utc_hour))
        # make sure directory exists before return
        if not os.path.exists(download_dir): os.makedirs(download_dir)
        return download_dir

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def analysisGribFilename(self, utc_hour, variable, **kwargs):
        template = self.getDownloadFileTemplate(self.analysis_config)
        template_args = dict(kwargs)
        template_args['date'] = self.timeToDirpath(utc_hour)
        template_args['source'] = self.sourceToFilepath(self.ndfd_config)
        template_args['variable'] = variable
        return template % template_args

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    def analysisGribFilepath(self, ndfd_config, utc_hour, time_span,
                                   variable, **kwargs):
        anal_dir = self.analysisDownloadDir(utc_hour)
        filename = \
           self.analysisGribFilename(utc_hour, time_span, variable, **kwargs)
        return os.path.join(anal_dir, filename)

