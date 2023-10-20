package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.XdbIdDAO;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.XdbId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Akhilanand K
 * @since 10/17/2023
 * wrapper to handle all DAO code
 */
public class RatGtexDAO {

    XdbIdDAO xdao = new XdbIdDAO();
    GeneDAO gdao = new GeneDAO();

    Logger logInserted = LogManager.getLogger("inserted");
    Logger logDeleted = LogManager.getLogger("deleted");

    public String getConnectionInfo() {
        return xdao.getConnectionInfo();
    }

    public List<XdbId> getRatGtexXdbIds(int speciesTypeKey,int ratGtexXdbKey, String srcPipeline) throws Exception {

        XdbId filter = new XdbId();
        filter.setXdbKey(ratGtexXdbKey);
        filter.setSrcPipeline(srcPipeline);
        return xdao.getXdbIds(filter, speciesTypeKey);
    }

    public List<XdbId> getGeneEnsmblXdbIdByRgdID(int geneRgdId) throws Exception {
        return xdao.getXdbIdsByRgdId(XdbId.XDB_KEY_ENSEMBL_GENES, geneRgdId);
    }

    /**
     * Returns all active genes for given species. Results do not contain splices or alleles
     * @param speciesKey species type key
     * @return list of active genes for given species
     * @throws Exception when unexpected error in spring framework occurs
     */
    public List<Gene> getActiveGenes(int speciesKey) throws Exception {
        return gdao.getActiveGenes(speciesKey);
    }

    /**
     * insert a bunch of XdbIds; duplicate entries are not inserted (with same RGD_ID,XDB_KEY,ACC_ID,SRC_PIPELINE)
     * @param xdbs list of XdbIds objects to be inserted
     * @return number of actually inserted rows
     * @throws Exception when unexpected error in spring framework occurs
     */
    public int insertXdbs(List<XdbId> xdbs) throws Exception {

        int rows = xdao.insertXdbs(xdbs);

        for( XdbId xdbId: xdbs ) {
            logInserted.debug(xdbId.dump("|"));
        }

        return rows;
    }

    /**
     * delete a list external ids (RGD_ACC_XDB rows);
     * if ACC_XDB_KEY is provided, it is used to delete the row;
     * else ACC_ID, RGD_ID, XDB_KEY and SRC_PIPELINE are used to locate and delete every row
     *
     * @param xdbIds list of external ids to be deleted
     * @return nr of rows deleted
     * @throws Exception when unexpected error in spring framework occurs
     */
    public int deleteXdbIds( List<XdbId> xdbIds ) throws Exception {

        for( XdbId xdbId: xdbIds ) {
            logDeleted.debug(xdbId.dump("|"));
        }

        return xdao.deleteXdbIds(xdbIds);
    }

    public int updateModificationDate(List<XdbId> xdbIds) throws Exception {

        List<Integer> xdbKeys = new ArrayList<Integer>(xdbIds.size());
        for( XdbId xdbId: xdbIds ) {
            xdbKeys.add(xdbId.getKey());
        }
        return xdao.updateModificationDate(xdbKeys);
    }
}
