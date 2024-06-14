<?xml version="1.0" encoding="UTF-8" ?>
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns="http://entsoe.eu/checks" version="1.0">
    <xsl:output method="xml" omit-xml-declaration="no" encoding="UTF-8" indent="yes" />
    <xsl:template match="/">
    
    <!--ROOT ELEMENT-->
        <xsl:element name = "QAReport">
            <xsl:attribute name="created" select="current-dateTime()"/>
            <xsl:attribute name="schemeVersion">
                <text>2.0</text>
            </xsl:attribute>
            <xsl:attribute name="serviceProvider">
                <text>BALTICRSC</text>
            </xsl:attribute>
            
            <xsl:for-each select="Result/ModelInformation">
            
                <!--<xsl:if test="Region!='ENTSO-E'">-->
                
                <xsl:variable name="LOADFLOW_CONVERGED" select="LoadFlowResultSummary/LoadFlowSummary/LoadFlow_Converged='Converged'" />
                <xsl:variable name="TOPOLOGY_CORRECT" select="TopologyValidation/Validated = 'true'" />
                    <!--IGM REPORT-->
                    <xsl:element name = "IGM">
                        <xsl:attribute name="created">
                            <xsl:value-of select="MetaData/creationDate"/>
                        </xsl:attribute>
                        <xsl:attribute name="processType">
                            <xsl:value-of select="MetaData/timeHorizon"/>
                        </xsl:attribute>
                        <xsl:attribute name="qualityIndicator">
                        <xsl:choose>
                            <xsl:when test="$LOADFLOW_CONVERGED and $TOPOLOGY_CORRECT">
                                <text>Plausible</text>
                            </xsl:when>
                            <xsl:otherwise>
                                <text>Substituted</text>
                            </xsl:otherwise>
                        </xsl:choose>
                        </xsl:attribute>
                        <xsl:attribute name="scenarioTime">
                            <xsl:value-of select="MetaData/scenarioDate"/>
                        </xsl:attribute>
                        <xsl:variable name="MAS" select="MetaData/modelingAuthoritySet" />
                        <xsl:attribute name="tso">
                            <xsl:choose>
                                <xsl:when test="MetaData/modelPartReference = ''">
                                    <xsl:value-of select="MetaData/modelPartReference"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:value-of select="MetaData/TSO"/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:attribute>
                        <xsl:attribute name="version">
                            <xsl:value-of select="MetaData/versionNumber"/>
                        </xsl:attribute>

                        <!--REFERENCES-->
                        <xsl:for-each select = "MetaData/Component">
                            <xsl:element name = "resource">
                                <xsl:value-of select="modelid"/>
                            </xsl:element>
                        </xsl:for-each>

                        <!--ERRORS-->
        				<xsl:if test="not($LOADFLOW_CONVERGED)">
							<RuleViolation validationLevel="8" ruleId="IGMConvergence" severity="ERROR">
								<Message>MSG 52. ERROR: IGM for <xsl:value-of select="$MAS"/> did not converge.</Message>
							</RuleViolation>
						</xsl:if>
                        
                        <xsl:if test="not($TOPOLOGY_CORRECT)">
                            <xsl:for-each select="TopologyValidation/WrongTopoNode">
                                <RuleViolation validationLevel="8" ruleId="WrongTopology" severity="ERROR">
        							<Message>TOPOLOGY ERROR: Wrong topology at node [<xsl:value-of select="."/>]</Message>
    							</RuleViolation>
                            </xsl:for-each>
                        </xsl:if>
                                              
                    </xsl:element>
               <!-- </xsl:if> -->
               
            </xsl:for-each>
                          
        </xsl:element>
        
    </xsl:template>
</xsl:transform>
